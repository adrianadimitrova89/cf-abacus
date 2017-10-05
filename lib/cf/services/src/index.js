'use strict';

const _ = require('underscore');
const extend = _.extend;

const cluster = require('abacus-cluster');
const oauth = require('abacus-oauth');
const perf = require('abacus-perf');
const router = require('abacus-router');
const throttle = require('abacus-throttle');
const webapp = require('abacus-webapp');
const retry = require('abacus-retry');
const yieldable = require('abacus-yieldable');
const functioncb = yieldable.functioncb;
const createReporter = require('abacus-client');
const createCarryOver = require('abacus-carryover');
const urienv = require('abacus-urienv');

const createEventReader = require('abacus-bridge/src/event-reader');
const dbcache = require('abacus-bridge/src/dbcache');
const createProgress = require('abacus-bridge/src/event-progress');
const createOrgFilter = require('abacus-bridge/src/org-filter');
const createDelayGenerator = require('abacus-bridge/src/delay-generator');
const retrieveToken = require('abacus-bridge/src/token-retriever');
const execute = require('abacus-bridge/src/executor');

const createEventBridge = require('./event-bridge');
const createServiceEventsURL = require('./service-events-url');
const serviceGuidClient = require('./service-guid-client');
const buildStatistics = require('./statistics');
const config = require('./config');
const createServiceFilter = require('./service-filter');
const convertEvent = require('./service-event-converter');

const debug = require('abacus-debug')('abacus-cf-services');
const edebug = require('abacus-debug')('e-abacus-cf-services');

const statistics = buildStatistics({
  cache: dbcache.createStatistics()
});

const uris = urienv({
  auth_server: 9882
});

const errors = {
  missingToken: false,
  noReportEverHappened: true,
  consecutiveReportFailures: 0,
  lastError: '',
  lastErrorTimestamp: ''
};

const perfUsageIdentifier = 'usage';

const extractGuids = (services) =>
  Object.keys(services).map((key) => services[key].guid);

const injectServiceGuids = yieldable((services, cfAdminToken, cb) => {
  const client = serviceGuidClient.create(cfAdminToken, perf, statistics);

  const retryInjectGuids = retry(client.injectGuids, retry.forever);
  retryInjectGuids(services, cb);
});

const createProgressCache = (cfg) => {
  const dbConfig = extend(cfg.db, {
    documentId: 'abacus-cf-services-cache'
  });
  return dbcache(dbConfig, statistics.cache);
};

const createUsageReporter = (reporter, abacusUsageToken) => {
  return {
    report: (usage, cb) => {
      reporter.reportUsage(usage, abacusUsageToken, cb);
    }
  };
};

const noopBrige = {
  start: (cb) => cb(),
  stop: (cb) => cb()
};

const bridge = (cfg) => {
  if (!cluster.isWorker())
    return noopBrige;

  let bridge;

  const start = (cb) => {
    debug('Starting service bridge worker');

    functioncb(function *() {
      const cache = createProgressCache(cfg);
      const progress = createProgress(cache, cfg.polling.events.lastKnownGUID);
      yield progress.load();

      const cfAdminToken = yield retrieveToken({
        authServerURI: uris.auth_server,
        clientId: cfg.cf.clientID,
        clientSecret: cfg.cf.clientSecret
      });

      const abacusUsageToken = cfg.oauth.enabled ? yield retrieveToken({
        authServerURI: uris.auth_server,
        clientId: cfg.system.clientID,
        clientSecret: cfg.system.clientSecret,
        scopes: ['abacus.usage.write', 'abacus.usage.read']
      }) : undefined;

      const services = cfg.services;
      yield injectServiceGuids(services, cfAdminToken);
      const serviceGuids = extractGuids(services);

      const createServiceEventReader = (guid) => {
        const url = createServiceEventsURL({
          serviceGuids,
          afterGuid: guid
        });

        return createEventReader({
          url,
          token: cfAdminToken,
          minAge: cfg.polling.events.minAge,
          statistics
        });
      };

      const eventFilters = [];
      if (cfg.polling.orgs)
        eventFilters.push(createOrgFilter(cfg.polling.orgs));
      if (cfg.services)
        eventFilters.push(createServiceFilter(cfg.services));

      const reporter = createReporter(statistics, errors);
      const registerError = reporter.registerError;
      const carryOver = createCarryOver(statistics, registerError);
      const usageReporter = createUsageReporter(reporter, abacusUsageToken);

      const delayGenerator = createDelayGenerator(
        cfg.polling.minInterval,
        cfg.polling.maxInterval
      );

      bridge = createEventBridge({
        createEventReader: createServiceEventReader,
        eventFilters,
        convertEvent,
        usageReporter,
        carryOver,
        progress,
        delayGenerator
      });
      bridge.on('usage.conflict', (operationStart) => {
        statistics.usage.conflicts++;
        perf.report(perfUsageIdentifier, operationStart);
      });
      bridge.on('usage.skip', (operationStart) => {
        statistics.usage.skips++;
        perf.report(perfUsageIdentifier, operationStart);
      });
      bridge.on('usage.failure', (err, operationStart) => {
        statistics.usage.failures++;
        registerError('Error reporting usage',
          error, undefined, perfUsageIdentifier, operationStart);
      });
      bridge.on('usage.success', (operationStart) => {
        statistics.usage.success++;
        perf.report(perfUsageIdentifier, operationStart);
      });
      yield yieldable(bridge.start)();
    })(cb);
  };

  const stop = (cb) => {
    if (bridge)
      bridge.stop(cb);
  };

  return {
    start,
    stop
  };
};

const createRoutes = (cfg) => {
  const routes = router();
  const cache = createProgressCache(cfg);
  const progress = createProgress(cache, cfg.polling.events.lastKnownGUID);
  routes.get('/v1/cf/stats', throttle(function *(req, res) {
    const doc = yield progress.load();
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
  return routes;
};

const web = (cfg) => {
  let server = null;

  const start = (cb) => {
    const app = webapp();

    if(cfg.oauth.enabled)
      app.use(/^\/v1\/cf\/stats/,
        oauth.validator(cfg.oauth.jwtKey, cfg.oauth.jwtAlgorithm));

    app.use(createRoutes(cfg));

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

  const cfg = config.loadFromEnvironment();

  const reporter = bridge(cfg);
  execute(reporter)
    .on('start-success', () => {
      debug('Started background job.');
    })
    .on('start-failure', (err) => {
      edebug('Failed to start background job: %s', err);
      throw err;
    })
    .on('stop-success', () => {
      debug('Stopped background job.');
    })
    .on('stop-failure', (err) => {
      edebug('Failed to stop background job: %s', err);
      throw err;
    });

  const rest = web(cfg);
  execute(rest)
    .on('start-success', () => {
      debug('Started server.');
    })
    .on('start-failure', (err) => {
      edebug('Failed to start server: %s', err);
      throw err;
    })
    .on('stop-success', () => {
      debug('Stopped server.');
    })
    .on('stop-failure', (err) => {
      edebug('Failed to stop server: %s', err);
      throw err;
    });
};

module.exports.web = web;
module.exports.bridge = bridge;
module.exports.runCLI = runCLI;
