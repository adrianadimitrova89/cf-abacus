'use strict';
/* eslint no-use-before-define: 0 */

const cluster = require('abacus-cluster');
const oauth = require('abacus-oauth');
const perf = require('abacus-perf');
const router = require('abacus-router');
const throttle = require('abacus-throttle');
const webapp = require('abacus-webapp');
const expGeneratorCreator = require('./exponential-generator');
const serviceEventChecker = require('./service-event-checker');
const serviceEventsClient = require('./service-events-client');
const serviceGuidClient = require('./service-guid-client');
const serviceUsageBuilder = require('./service-usage-builder');
const buildStatistics = require('./statistics');
const tokensProviderCreator = require('./tokens-provider');
const config = require('./config');
const dbcache = require('./dbcache');
const _ = require('underscore');
const extend = _.extend;
const async = require('async');
const retry = require('abacus-retry');

const debug = require('abacus-debug')('abacus-cf-services');
const edebug = require('abacus-debug')('e-abacus-cf-services');

const statistics = buildStatistics({
  cache: dbcache.createStatistics()
});

const cfg = config.loadFromEnvironment();

const reportingConfig = (() => {
  const minIntervalTime = parseInt(process.env.MIN_INTERVAL_TIME) || 5000;
  const maxIntervalTime = parseInt(process.env.MAX_INTERVAL_TIME) || 240000;
  const orgsToReport = process.env.ORGS_TO_REPORT ?
    JSON.parse(process.env.ORGS_TO_REPORT) : undefined;

  return {
    minInterval    : minIntervalTime,
    maxInterval    : maxIntervalTime,
    orgsToReport   : orgsToReport
  };
})();

const generator = expGeneratorCreator.create(
  reportingConfig.minInterval,
  reportingConfig.maxInterval);


const errors = {
  missingToken: false,
  noReportEverHappened: true,
  consecutiveReportFailures: 0,
  lastError: '',
  lastErrorTimestamp: ''
};

const reporter = require('abacus-client')(statistics, errors);
const registerError = reporter.registerError;
const carryOver = require('abacus-carryover')(statistics, registerError);

const serviceGuids = () => {
  return Object.keys(cfg.services)
    .map((key) => cfg.services[key].guid)
    .filter((guid) => guid != undefined);
};

const lastRecordedCache = (() => {
  const dbConfig = extend(cfg.db, {
    documentId: 'abacus-cf-services-cache'
  });
  return dbcache.create(dbConfig, statistics.cache);
})();

const getLastRecordedDoc = (cb) => {
  lastRecordedCache.read((err, doc) => {
    if (err) {
      cb(err);
      return;
    }

    if (doc)
      cb(undefined, doc);
    else
      cb(undefined, {
        lastRecordedGUID: cfg.polling.events.lastKnownGUID,
        lastRecordedTimestamp: undefined
      });
  });
};

const sendUsage = (usage, abacusUsageToken, event, cb) => {
  reporter.reportUsage(usage, abacusUsageToken, (error, response) => {
    if (!error && response && response.statusCode === 201)
      carryOver.write(usage, response, event.metadata.guid, event.entity.state,
        (error) => {
          cb(error, response);
        });
    else
      cb(error, response);
  });
};

const validateAbacusResponse = (response, callback) => {
  if (response && response.statusCode === 409) {
    callback();
    return;
  }

  if (!response || response.statusCode !== 201) {
    // TODO fix message
    callback(new Error('Error reporting usage... something here'));
    return;
  }

  callback();
};

const storeLastProcessedGuid = (event, callback) => {
  lastRecordedCache.write({
    lastRecordedGUID: event.metadata.guid,
    lastRecordedTimestamp: event.metadata.created_at
  }, callback);
};

const reportServiceUsage = (lastDocGuid, cfAdminToken, abacusUsageToken) => {
  errors.missingToken = false;

  const eventChecker = serviceEventChecker.create(cfg.services);
  const usageBuilder = serviceUsageBuilder.create(statistics,
    reporter.registerError, eventChecker);

  const eventsClient = serviceEventsClient.create({
    cfAdminToken,
    perf,
    statistics,
    minAge: cfg.polling.events.minAge,
    orgsToReport: cfg.polling.orgs
  });

  const retriever = eventsClient.retriever({
    serviceGuids: serviceGuids(),
    afterGuid: lastDocGuid
  });

  let lastProcessedEventGuid = lastDocGuid;

  retriever.forEachEvent((event, done) => {
    // const t0 = moment.now();
    debug('Processing event: %s', event.metadata.guid);

    async.waterfall([
      (callback) => usageBuilder.buildServiceUsage(event, callback),
      (usage, callback) => sendUsage(usage, abacusUsageToken, event, callback),
      (response, callback) => validateAbacusResponse(response, callback),
      (callback) => storeLastProcessedGuid(event, callback)
    ], (err) => {
      debug('Finished processing event: %s', event.metadata.guid);
      if (!err || err === serviceUsageBuilder.unsupportedEventError) {
        lastProcessedEventGuid = event.metadata.guid;
        done();
        return;
      }

      done(err);
    });

  });

  retriever.whenSucceeded(() => {
    debug('Reporting service usage finished successfully');
    generator.reset();
    scheduleReportServiceUsage(lastProcessedEventGuid,
      cfAdminToken, abacusUsageToken, generator.getNext());
  });

  retriever.whenFailed((err) => {
    edebug('Error while processing service events. Error: ', err);

    if (err === serviceEventsClient.guidNotFoundError) {
      edebug('CC cannot find GUID %s. Restarting reporting',
        lastProcessedEventGuid);
      scheduleReportServiceUsage(undefined,
        cfAdminToken, abacusUsageToken, generator.getNext());
      return;
    }

    scheduleReportServiceUsage(lastProcessedEventGuid,
       cfAdminToken, abacusUsageToken, generator.getNext());
  });

  retriever.start();
};

const scheduleReportServiceUsage =
  (lastDocGuid, cfAdminToken, abacusUsageToken, afterTimeout) => {
    debug('Scheduling next service report after %s milliseconds. ' +
    'Last processed guid: %s', afterTimeout, lastDocGuid);
    setTimeout(() => reportServiceUsage(lastDocGuid,
       cfAdminToken, abacusUsageToken), afterTimeout);
  };

const stopReporting = (cb = () => {}) => {
  edebug('Cancelling timers');
  clearTimeout(module.usageReporter);
  cb();
};

const retryLoadServiceGuids = (cfAdminToken, cb) => {
  const client = serviceGuidClient.create(cfAdminToken, perf, statistics);
  const retryInjectGuids = retry(client.injectGuids, retry.forever);

  retryInjectGuids(cfg.services, cb);
};

const routes = router();
routes.get('/v1/cf/stats', throttle(function *(req) {
  return {
    body: {
      services: {
        config: {
          secured: cfg.oauth.enabled,
          minIntervalTime: reportingConfig.minInterval,
          maxIntervalTime: reportingConfig.maxInterval,
          guidMinAge: cfg.polling.events.minAge,
          reporting: reportingConfig,
          orgsToReport: cfg.polling.orgs
        },
        // FIXME: lastRecordedCache
        cache: 'lastRecordedCache',
        performance: {
          cache: {
            read: perf.stats('cache.read'),
            write: perf.stats('cache.write')
          },
          paging: {
            pages: perf.stats('paging'),
            resources: perf.stats('paging.resources')
          },
          report: perf.stats('report'),
          usage: perf.stats('usage'),
          carryOver: perf.stats('carryOver')
        },
        statistics: statistics,
        errors: errors
      }
    }
  };
}));

const bridge = () => {
  debug('Starting service bridge app ...');
  cluster.singleton();

  if (cluster.isWorker()) {
    debug('Starting service bridge worker');
    // TODO make config in this format
    const tokensProvider = tokensProviderCreator.create({
      cfadmin: {
        clientId: cfg.cf.clientID,
        clientSecret: cfg.cf.clientSecret
      },
      usage: {
        clientId: cfg.system.clientID,
        clientSecret: cfg.system.clientSecret
      },
      secured: cfg.oauth.enabled
    });

    getLastRecordedDoc((err, lastDoc) => {
      if (err)
        throw err;

      tokensProvider.getStartedTokens((cfAdminToken, abacusUsageToken) => {
        retryLoadServiceGuids(cfAdminToken, () =>
          scheduleReportServiceUsage(lastDoc.lastRecordedGUID,
            cfAdminToken, abacusUsageToken, generator.getNext()));
      });
    });
  }

  process.on('exit', () => {
    stopReporting();
  });

  const app = webapp();

  if(cfg.oauth.enabled)
    app.use(/^\/v1\/cf\/stats/,
      oauth.validator(cfg.oauth.jwtKey, cfg.oauth.jwtAlgorithm));

  app.use(routes);

  return app;
};

const runCLI = () => bridge().listen();

module.exports = bridge;
module.exports.runCLI = runCLI;
