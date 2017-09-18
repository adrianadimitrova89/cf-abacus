'use strict';

const cluster = require('abacus-cluster');
const moment = require('abacus-moment');
const oauth = require('abacus-oauth');
const perf = require('abacus-perf');
const router = require('abacus-router');
const throttle = require('abacus-throttle');
const urienv = require('abacus-urienv');
const webapp = require('abacus-webapp');
const expGeneratorCreator = require('./exponential-generator');
const serviceEventChecker = require('./service-event-checker');
const serviceEventsClient = require('./service-events-client');
const serviceGuidClient = require('./service-guid-client');
const serviceUsageBuilder = require('./service-usage-builder');
const config = require('./config');
const dbcache = require('./dbcache');
const _ = require('underscore');
const extend = _.extend;
const memoize = _.memoize;
const async = require('async');
const retry = require('abacus-retry');

const debug = require('abacus-debug')('abacus-cf-services');
const edebug = require('abacus-debug')('e-abacus-cf-services');

const cfg = config.loadFromEnvironment();

// Resolve service URIs
const uris = memoize(() => urienv({
  auth_server: 9882,
  [cfg.db.alias]: 5984
}));

const cfAdminToken = oauth.cache(
  uris().auth_server, cfg.cf.clientID, cfg.cf.clientSecret);

const systemScopes = ['abacus.usage.write', 'abacus.usage.read'];

const serviceUsageToken = cfg.oauth.enabled
  ? oauth.cache(
      uris().auth_server,
      cfg.system.clientID,
      cfg.system.clientSecret,
      systemScopes.join(' '))
  : undefined;

const reportingConfig = (() => {
  const minIntervalTime = parseInt(process.env.MIN_INTERVAL_TIME) || 5000;
  const maxIntervalTime = parseInt(process.env.MAX_INTERVAL_TIME) || 240000;
  const orgsToReport = process.env.ORGS_TO_REPORT ?
    JSON.parse(process.env.ORGS_TO_REPORT) : undefined;

  return {
    minInterval    : minIntervalTime,
    maxInterval    : maxIntervalTime,
    // TODO: we should not need maxRetries!!
    maxRetries     : Math.floor(
      Math.log(maxIntervalTime - minIntervalTime + 1)),
    orgsToReport   : orgsToReport
  };
})();

const generator = expGeneratorCreator.create(
  reportingConfig.minInterval,
  reportingConfig.maxInterval);


// Function call statistics
const statistics = {
  cache: dbcache.createStatistics(),
  usage: {
    missingToken: 0,
    reportFailures: 0,
    reportSuccess: 0,
    reportConflict: 0,
    reportBusinessError: 0,
    loopFailures: 0,
    loopSuccess: 0,
    loopConflict: 0,
    loopSkip : 0
  },
  carryOver: {
    getSuccess   : 0,
    getNotFound  : 0,
    getFailure   : 0,
    removeSuccess: 0,
    removeFailure: 0,
    upsertSuccess: 0,
    upsertFailure: 0,
    readSuccess  : 0,
    readFailure  : 0,
    docsRead     : 0
  },
  paging: {
    missingToken: 0,
    pageReadSuccess: 0,
    pageReadFailures: 0,
    pageProcessSuccess: 0,
    pageProcessFailures: 0,
    pageProcessEnd: 0
  }
};

const errors = {
  missingToken: false,
  noReportEverHappened: true,
  consecutiveReportFailures: 0,
  lastError: '',
  lastErrorTimestamp: ''
};

const eventsClient = serviceEventsClient.create({
  cfAdminToken,
  perf,
  statistics,
  minAge: cfg.polling.events.minAge,
  orgsToReport: cfg.polling.orgs
});

// Initialize reporter with statistics, errors and get errors function
const reporter = require('abacus-client')(statistics, errors);
const registerError = reporter.registerError;

// Initialize carryOver with statistics and error function
const carryOver = require('abacus-carryover')(statistics, registerError);


const serviceGuids = () => {
  return Object.keys(cfg.services)
    .map((key) => cfg.services[key].guid)
    .filter((guid) => guid != undefined);
};

let lastRecordedDoc = {
  lastRecordedGUID: cfg.polling.events.lastKnownGUID,
  lastRecordedTimestamp: undefined
};

const lastRecordedCache = (() => {
  const dbConfig = extend(cfg.db, {
    documentID: 'abacus-cf-services-cache'
  });
  return dbcache.create(dbConfig, statistics.cache);
})();

const initLastRecordedDoc = (cb) => {
  lastRecordedCache.read((err, doc) => {
    if (err)
      cb(err);

    if (doc)
      lastRecordedDoc = doc;

    cb();
  });
};

const updateLastRecordedDoc = (cb) => {
  lastRecordedCache.write(lastRecordedDoc, (err) => {
    if (err)
      cb(err);
    cb();
  });
};

// UP TO HERE ......................................

const reportUsage = (usage, guid, state, token, cb = () => {}) => {
  reporter.reportUsage(usage, token, (error, response) => {
    if (!error && response && response.statusCode === 201)
      carryOver.write(usage, response, guid, state, (error) => {
        cb(error, response);
      });
    else
      cb(error, response);
  });
};

/**
 * Remember the resource GUID. We will poll events from CC starting with the
 * last stored GUID.
 */
const storeGuid = (resource) => {
  const resourceDate = moment.utc(resource.metadata.created_at);
  lastRecordedDoc.lastRecordedGUID = resource.metadata.guid;
  lastRecordedDoc.lastRecordedTimestamp = resource.metadata.created_at;
  debug('Last processed guid set to %s with time-stamp %s',
    lastRecordedDoc.lastRecordedGUID, resourceDate.toISOString());
};

const reportServiceUsage = (cfToken, abacusToken) => {
  errors.missingToken = false;

  const eventChecker = serviceEventChecker.create(cfg.services);
  const usageBuilder = serviceUsageBuilder.create(statistics,
    reporter.registerError, eventChecker);

  const retriever = eventsClient.retriever({
    serviceGuids: serviceGuids(),
    afterGuid: lastRecordedDoc.lastRecordedGUID
  });

  retriever.forEachEvent((event, done) => {

    // const t0 = moment.now();
    const unsupportedEventError = new Error('Unsupported event type');

    async.waterfall([
      (callback) => usageBuilder.buildServiceUsage(event, (err, usage) => {
        // comment
        callback(err, usage);
      }),
      (usage, callback) => {
        if (!usage) {
          callback(unsupportedEventError);
          return;
        }

        reportUsage(usage, event.metadata.guid, event.entity.state,
          abacusToken, callback);
      },
      (response, callback) => {
        if (response && response.statusCode === 409) {
          callback();
          return;
        }

        if (!response || response.statusCode !== 201) {
          // TODO fix message
          callback(new Error('Error reporting usage... something here'));
          return;
        }

        storeGuid(event);
        updateLastRecordedDoc(callback);
      }
    ], (err) => {
      if (err === unsupportedEventError) {
        done();
        return;
      }

      done(err);
    });

  });

  retriever.whenFinished((err) => {
    if (err) {
      edebug('Error while processing service events. Error: ', err);

      if (err === serviceEventsClient.guidNotFoundError) {
        edebug('CC cannot find GUID %s. Restarting reporting',
          lastRecordedDoc.lastRecordedGUID);
        lastRecordedDoc.lastRecordedGUID = undefined;
        lastRecordedDoc.lastRecordedTimestamp = undefined;
      }
      setTimeout(
        () => reportServiceUsage(cfToken, abacusToken),
        generator.getNext());
    }
    else {
      debug('Reporting service usage finished successfully');
      generator.reset();
      setTimeout(
        () => reportServiceUsage(cfToken, abacusToken),
        generator.getNext());
    }
  });

  retriever.start();


};

const stopReporting = (cb = () => {}) => {
  edebug('Cancelling timers');
  clearTimeout(module.usageReporter);
  cb();
};

const loadServiceGuidsConfiguration = (cb) => {
  const client = serviceGuidClient.create(cfAdminToken, perf, statistics);
  const retryInjectGuids = retry(client.injectGuids, retry.forever);

  retryInjectGuids(cfg.services, cb);
};

const startTokens = (cb) => {
  const retryStartAdminToken = retry(cfAdminToken.start, retry.forever);
  retryStartAdminToken((err) => {
    debug('Succesfully fetched admin token ...');
    if (cfg.oauth.enabled) {
      const retryStartUsageToken = retry(serviceUsageToken.start,
        retry.forever);
      retryStartUsageToken(cb);
      return;
    }

    cb();
  });
};

const scheduleUsageReporting = () => {
  loadServiceGuidsConfiguration(() =>
    reportServiceUsage(cfAdminToken, serviceUsageToken));
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
        cache: lastRecordedCache,
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

    initLastRecordedDoc((err) => {
      if (err)
        throw err;

      startTokens((err) => {
        scheduleUsageReporting();
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
module.exports.statistics = statistics;
module.exports.reportingConfig = reportingConfig;
module.exports.errors = errors;
module.exports.reportServiceUsage = reportServiceUsage;
module.exports.stopReporting = stopReporting;
module.exports.runCLI = runCLI;
