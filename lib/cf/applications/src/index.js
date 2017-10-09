'use strict';

const _ = require('underscore');
const extend = _.extend;

const oauth = require('abacus-oauth');
const perf = require('abacus-perf');
const router = require('abacus-router');
const throttle = require('abacus-throttle');
const webapp = require('abacus-webapp');
const yieldable = require('abacus-yieldable');
const functioncb = yieldable.functioncb;
const createReporter = require('abacus-client');
const createCarryOver = require('abacus-carryover');

const retrieveToken = require('abacus-bridge/src/token-retriever');
const dbcache = require('abacus-bridge/src/dbcache');
const createProgress = require('abacus-bridge/src/event-progress');
const createEventReader = require('abacus-bridge/src/event-reader');
const createOrgFilter = require('abacus-bridge/src/org-filter');
const createDelayGenerator = require('abacus-bridge/src/delay-generator');
const createEventBridge = require('abacus-bridge/src/event-bridge');
const runApplication = require('abacus-bridge/src/application');
const buildStatistics = require('abacus-bridge/src/statistics');
const config = require('abacus-bridge/src/config');

const createAppEventsURL = require('./app-events-url');
const convertEvent = require('./app-event-converter');

const debug = require('abacus-debug')('abacus-cf-applications');
const edebug = require('abacus-debug')('e-abacus-cf-applications');

const perfUsageIdentifier = 'usage';

const createProgressCache = (cfg, statistics) => {
  const dbConfig = extend(cfg.db, {
    documentId: 'abacus-cf-bridge-cache'
  });
  return dbcache(dbConfig, statistics.cache);
};

const loadCFAdminToken = function *(cfg) {
  return yield retrieveToken({
    authServerURI: cfg.cf.url,
    clientId: cfg.cf.clientID,
    clientSecret: cfg.cf.clientSecret
  });
};

const loadCollectorToken = function *(cfg) {
  if (!cfg.oauth.enabled)
    return undefined;
  return yield retrieveToken({
    authServerURI: cfg.cf.url,
    clientId: cfg.collector.clientID,
    clientSecret: cfg.collector.clientSecret,
    scopes: [
      'abacus.usage.linux-container.write',
      'abacus.usage.linux-container.read'
    ]
  });
};

const createUsageReporter = (reporter, token) => {
  return {
    report: (usage, cb) => {
      reporter.reportUsage(usage, token, cb);
    }
  };
};

const load = function *() {
  const cfg = config.loadFromEnvironment();

  const statistics = buildStatistics({
    cache: dbcache.createStatistics()
  });

  const errors = {
    missingToken: false,
    noReportEverHappened: true,
    consecutiveReportFailures: 0,
    lastError: '',
    lastErrorTimestamp: ''
  };

  const cache = createProgressCache(cfg, statistics);
  const progress = createProgress(cache, cfg.polling.events.lastKnownGUID);
  yield progress.load();

  return {
    cfg,
    progress,
    statistics,
    errors
  };
};

const createEventFilters = (cfg) => {
  const filters = [];
  if (cfg.polling.orgs)
    filters.push(createOrgFilter(cfg.polling.orgs));
  return filters;
};

const createJob = function *(opts) {
  const cfg = opts.cfg;

  const cfAdminToken = yield loadCFAdminToken(cfg);
  const collectorToken = yield loadCollectorToken(cfg);

  const createAppEventReader = (guid) => {
    const url = createAppEventsURL({
      afterGuid: guid
    });

    return createEventReader({
      url,
      token: cfAdminToken,
      minAge: cfg.polling.events.minAge,
      statistics: opts.statistics
    });
  };

  const reporter = createReporter(opts.statistics, opts.errors);
  const registerError = reporter.registerError;
  const carryOver = createCarryOver(opts.statistics, registerError);
  const usageReporter = createUsageReporter(reporter, collectorToken);

  const delayGenerator = createDelayGenerator(
    cfg.polling.minInterval,
    cfg.polling.maxInterval
  );

  const bridge = createEventBridge({
    createEventReader: createAppEventReader,
    eventFilters: createEventFilters(cfg),
    convertEvent,
    usageReporter,
    carryOver,
    progress: opts.progress,
    delayGenerator
  });
  bridge.on('usage.conflict', (operationStart) => {
    opts.statistics.usage.conflicts++;
    perf.report(perfUsageIdentifier, operationStart);
  });
  bridge.on('usage.skip', (operationStart) => {
    opts.statistics.usage.skips++;
    perf.report(perfUsageIdentifier, operationStart);
  });
  bridge.on('usage.failure', (err, operationStart) => {
    opts.statistics.usage.failures++;
    registerError('Error reporting usage',
      err, undefined, perfUsageIdentifier, operationStart);
  });
  bridge.on('usage.success', (operationStart) => {
    opts.statistics.usage.success++;
    perf.report(perfUsageIdentifier, operationStart);
  });
  return bridge;
};

const createRoutes = (opts) => {
  const routes = router();
  routes.get('/v1/cf/applications', throttle(function *(req) {
    debug('Getting applications bridge info');
    return {
      body: {
        applications: {
          config: opts.cfg,
          cache: opts.progress.get(),
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
          statistics: opts.statistics,
          errors: opts.errors
        }
      }
    };
  }));
  return routes;
};

const createWeb = function *(opts) {
  const app = webapp();
  if (opts.cfg.oauth.enabled)
    app.use(/^\/v1\/cf\/applications/,
      oauth.validator(opts.cfg.oauth.jwtKey, opts.cfg.oauth.jwtAlgorithm));
  app.use(createRoutes(opts));

  let server;
  const start = (cb) => {
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

const runCLI = () => runApplication({
  load: functioncb(load),
  createJob: functioncb(createJob),
  createWeb: functioncb(createWeb)
}, (err) => {
  if (err) {
    edebug('Failed to initialize service bridge: ', err);
    throw err;
  }
  else
    debug('Service bridge initialized!');
});

module.exports.runCLI = runCLI;
