'use strict';

const _ = require('underscore');
const extend = _.extend;

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
const createEventBridge = require('abacus-bridge/src/event-bridge');
const runApplication = require('abacus-bridge/src/application');
const commonConfig = require('abacus-bridge/src/config');

const createServiceEventsURL = require('./service-events-url');
const createServiceGuidClient = require('./service-guid-client');
const buildStatistics = require('./statistics');
const config = require('./config');
const createServiceFilter = require('./service-filter');
const convertEvent = require('./service-event-converter');

const debug = require('abacus-debug')('abacus-cf-services');
const edebug = require('abacus-debug')('e-abacus-cf-services');

const perfUsageIdentifier = 'usage';

const extractGuids = (services) =>
  Object.keys(services).map((key) => services[key].guid);

const injectServiceGuids =
  yieldable((services, cfAdminToken, statistics, cb) => {
    const client = createServiceGuidClient(cfAdminToken, perf, statistics);

    const retryInjectGuids = retry(client.injectGuids, retry.forever);
    retryInjectGuids(services, cb);
  });

const createProgressCache = (cfg, statistics) => {
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

const load = function *() {
  const cfg = extend(
    commonConfig.loadFromEnvironment(),
    config.loadFromEnvironment()
  );

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
  const eventFilters = [];
  if (cfg.polling.orgs)
    eventFilters.push(createOrgFilter(cfg.polling.orgs));
  if (cfg.services)
    eventFilters.push(createServiceFilter(cfg.services));
  return eventFilters;
};

const loadCFAdminToken = function *(cfg) {
  const uris = urienv({ // TODO: Extract to cfg
    auth_server: 9882
  });
  return yield retrieveToken({
    authServerURI: uris.auth_server,
    clientId: cfg.cf.clientID,
    clientSecret: cfg.cf.clientSecret
  });
};

const loadAbacusUsageToken = function *(cfg) {
  if (!cfg.oauth.enabled)
    return undefined;
  const uris = urienv({ // TODO: Extract to cfg
    auth_server: 9882
  });
  return yield retrieveToken({
    authServerURI: uris.auth_server,
    clientId: cfg.collector.clientID,
    clientSecret: cfg.collector.clientSecret,
    scopes: ['abacus.usage.write', 'abacus.usage.read']
  });
};

const createJob = function *(opts) {
  const cfg = opts.cfg;

  const cfAdminToken = yield loadCFAdminToken(cfg);
  const abacusUsageToken = yield loadAbacusUsageToken(cfg);

  const services = cfg.services;
  yield injectServiceGuids(services, cfAdminToken, opts.statistics);
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
      statistics: opts.statistics
    });
  };

  const reporter = createReporter(opts.statistics, opts.errors);
  const registerError = reporter.registerError;
  const carryOver = createCarryOver(opts.statistics, registerError);
  const usageReporter = createUsageReporter(reporter, abacusUsageToken);

  const delayGenerator = createDelayGenerator(
    cfg.polling.minInterval,
    cfg.polling.maxInterval
  );

  const bridge = createEventBridge({
    createEventReader: createServiceEventReader,
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
  routes.get('/v1/cf/stats', throttle(function *(req) {
    return {
      body: {
        services: {
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
            usage: perf.stats(perfUsageIdentifier),
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
    app.use(/^\/v1\/cf\/stats/,
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
