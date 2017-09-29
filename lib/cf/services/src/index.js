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
const execute = require('./executor');
const moment = require('abacus-moment');
const yieldable = require('abacus-yieldable');

const debug = require('abacus-debug')('abacus-cf-services');
const edebug = require('abacus-debug')('e-abacus-cf-services');

const statistics = buildStatistics({
  cache: dbcache.createStatistics()
});

const cfg = config.loadFromEnvironment();

const generator = expGeneratorCreator.create(
  cfg.polling.minInterval,
  cfg.polling.maxInterval);

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

const conflictError = new Error('Conflicting usage event');
const perfUsageIdentifier = 'usage';

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

const isConflictingResponse = (response) => {
  return response && response.statusCode === 409;
};

const isErrorResponse = (response) => {
  if (!response)
    return true;
  const isCreated = response.statusCode === 201;
  const isDuplicate = response.statusCode === 409;
  return !isCreated && !isDuplicate;
};

const validateAbacusResponse = (response, callback) => {
  if (isErrorResponse(response))
    return callback(new Error('Error reporting usage!'));

  if (isConflictingResponse(response))
    return callback(conflictError);

  return callback();
};

const storeLastProcessedGuid = (event, callback) => {
  lastRecordedCache.write({
    lastRecordedGUID: event.metadata.guid,
    lastRecordedTimestamp: event.metadata.created_at
  }, callback);
};

const extractGuids = (services) =>
  Object.keys(services).map((key) => services[key].guid);

const trackReportingStatistics = (err, callback) => {
  if (err === conflictError) {
    statistics.usage.loopConflict++;
    return callback();
  }
  if (err === serviceUsageBuilder.unsupportedEventError) {
    statistics.usage.loopSkip++;
    return callback();
  }
  if (err) {
    statistics.usage.loopFailures++;
    return callback(err);
  }
  statistics.usage.loopSuccess++;
  return callback();
};

const reportServiceUsage = (lastDocGuid, cfAdminToken, abacusUsageToken) => {
  debug('Reporting service usage, starting from "%s"', lastDocGuid);

  const eventChecker = serviceEventChecker.create(cfg.services);
  const usageBuilder = serviceUsageBuilder.create(carryOver, eventChecker);

  const eventsClient = serviceEventsClient.create({
    cfAdminToken,
    statistics,
    minAge: cfg.polling.events.minAge,
    orgsToReport: cfg.polling.orgs
  });

  const retriever = eventsClient.retriever({
    serviceGuids: extractGuids(cfg.services),
    afterGuid: lastDocGuid
  });

  let lastProcessedEventGuid = lastDocGuid;

  retriever.forEachEvent((event, done) => {
    const reportStart = moment.now();

    async.waterfall([
      (callback) => usageBuilder.buildServiceUsage(event, callback),
      (usage, callback) => sendUsage(usage, abacusUsageToken, event, callback),
      (response, callback) => validateAbacusResponse(response, callback)
    ], (err) => {
      async.waterfall([
        (callback) => trackReportingStatistics(err, callback),
        (callback) => storeLastProcessedGuid(event, callback)
      ], (finalErr) => {
        if (finalErr) {
          registerError('Error processing usage',
            finalErr, undefined, perfUsageIdentifier, reportStart);
          return done(finalErr);
        }
        lastProcessedEventGuid = event.metadata.guid;
        perf.report(perfUsageIdentifier, reportStart);
        return done();
      });
    });
  });

  retriever.whenSucceeded(() => {
    debug('Reporting service usage finished successfully');
    generator.reset();
    scheduleReportServiceUsage(lastProcessedEventGuid,
      cfAdminToken, abacusUsageToken, generator.getNext());
  });

  retriever.whenFailed((err) => {
    edebug('Error while processing service events: ', err);

    if (err === serviceEventsClient.guidNotFoundError) {
      edebug('CC cannot find GUID "%s". ' +
        'Restarting reporting, starting from epoch.',
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

let reporterTimer;

const scheduleReportServiceUsage =
  (lastDocGuid, cfAdminToken, abacusUsageToken, afterTimeout) => {
    debug('Scheduling next service report after %s milliseconds. ' +
    'Last processed guid: %s', afterTimeout, lastDocGuid);
    reporterTimer = setTimeout(() => reportServiceUsage(lastDocGuid,
       cfAdminToken, abacusUsageToken), afterTimeout);
  };

const loadServiceGuids = (cfAdminToken, cb) => {
  const client = serviceGuidClient.create(cfAdminToken, perf, statistics);

  const retryInjectGuids = retry(client.injectGuids, retry.forever);
  retryInjectGuids(cfg.services, cb);
};

const routes = router();
routes.get('/v1/cf/stats', throttle(function *(req, res) {
  const doc = yield yieldable(getLastRecordedDoc)();
  return {
    body: {
      services: {
        config: cfg,
        cache: doc,
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
          usage: perf.stats(perfUsageIdentifier),
          carryOver: perf.stats('carryOver')
        },
        statistics: statistics,
        errors: errors
      }
    }
  };
}));

const bridge = () => {
  const start = (cb) => {
    if (!cluster.isWorker()) {
      cb();
      return;
    }

    debug('Starting service bridge worker');
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

    async.waterfall([
      (callback) => {
        getLastRecordedDoc(callback);
      },
      (lastDoc, callback) => {
        tokensProvider.getStartedTokens((cfAdminToken, abacusUsageToken) => {
          callback(undefined, lastDoc, cfAdminToken, abacusUsageToken);
        });
      },
      (lastDoc, cfAdminToken, abacusUsageToken, callback) => {
        loadServiceGuids(cfAdminToken, () => {
          callback(undefined, lastDoc, cfAdminToken, abacusUsageToken);
        });
      },
      (lastDoc, cfAdminToken, abacusUsageToken, callback) => {
        scheduleReportServiceUsage(lastDoc.lastRecordedGUID,
          cfAdminToken, abacusUsageToken, generator.getNext());
        callback();
      }
    ], (err) => {
      cb(err);
    });
  };

  const stop = (cb) => {
    clearTimeout(reporterTimer);
    cb();
  };

  return {
    start,
    stop
  };
};

const web = () => {
  let server = null;

  const start = (cb) => {
    const app = webapp();

    if(cfg.oauth.enabled)
      app.use(/^\/v1\/cf\/stats/,
        oauth.validator(cfg.oauth.jwtKey, cfg.oauth.jwtAlgorithm));

    app.use(routes);

    server = app.listen(undefined, cb);
  };

  const stop = (cb) => {
    if (server)
      server.close(cb);
    else
      cb();
  };

  return {
    start,
    stop
  };
};

const runCLI = () => {
  cluster.singleton();

  const reporter = bridge();
  execute(reporter)
    .on('start-success', () => {
      debug('Successfully started Reporter part of bridge.');
    })
    .on('start-failure', (err) => {
      debug('Failed to start Reporter part of bridge: %s', err);
      throw err;
    })
    .on('stop-success', () => {
      debug('Successfully stopped Reporter part of bridge.');
    })
    .on('stop-failure', (err) => {
      debug('Failed to stop Reporter part of bridge: %s', err);
      throw err;
    });

  const rest = web();
  execute(rest)
    .on('start-success', () => {
      debug('Successfully started REST part of bridge.');
    })
    .on('start-failure', (err) => {
      debug('Failed to start REST part of bridge: %s', err);
      throw err;
    })
    .on('stop-success', () => {
      debug('Successfully stopped REST part of bridge.');
    })
    .on('stop-failure', (err) => {
      debug('Failed to stop REST part of bridge: %s', err);
      throw err;
    });
};

module.exports.web = web;
module.exports.bridge = bridge;
module.exports.runCLI = runCLI;
