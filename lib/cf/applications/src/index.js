'use strict';

const _ = require('underscore');
const memoize = _.memoize;

const oauth = require('abacus-oauth');
const perf = require('abacus-perf');
const router = require('abacus-router');
const throttle = require('abacus-throttle');
const urienv = require('abacus-urienv');
const webapp = require('abacus-webapp');
const yieldable = require('abacus-yieldable');
const functioncb = yieldable.functioncb;

const retrieveToken = require('abacus-bridge/src/token-retriever');
const dbcache = require('abacus-bridge/src/dbcache');
const createProgress = require('abacus-bridge/src/event-progress');
const createEventReader = require('abacus-bridge/src/event-reader');
const createOrgFilter = require('abacus-bridge/src/org-filter');
const createDelayGenerator = require('abacus-bridge/src/delay-generator');
const createEventBridge = require('abacus-bridge/src/event-bridge');
const runApplication = require('abacus-bridge/src/application');

const createAppEventsURL = require('./app-events-url');
const convertEvent = require('./app-event-converter');

const debug = require('abacus-debug')('abacus-cf-applications');
const edebug = require('abacus-debug')('e-abacus-cf-applications');

const perfUsageIdentifier = 'usage';

const dbalias = process.env.DBALIAS || 'db';

// Resolve service URIs
const uris = memoize(() => urienv({
  auth_server: 9882,
  [dbalias]  : 5984
}));

// Use secure routes or not
const secured = process.env.SECURED === 'true';

let cfAdminToken = undefined;
let linuxContainerToken = undefined;

const minIntervalTime = parseInt(process.env.MIN_INTERVAL_TIME) || 5000;
const maxIntervalTime = parseInt(process.env.MAX_INTERVAL_TIME) || 240000;
const guidMinAge = parseInt(process.env.GUID_MIN_AGE) || 60000;
const orgsToReport = process.env.ORGS_TO_REPORT ?
  JSON.parse(process.env.ORGS_TO_REPORT) : undefined;
const lastRecordedGuid = process.env.LAST_RECORDED_GUID;

const reportingConfig = {
  minInterval     : minIntervalTime,
  maxInterval     : maxIntervalTime,
  guidMinAge      : guidMinAge,
  maxRetries      : Math.floor(Math.log(maxIntervalTime)),
  orgsToReport    : orgsToReport,
  lastRecordedGUID: lastRecordedGuid
};


// Function call statistics
const statistics = {
  cache: {
    readSuccess: 0,
    readFailure: 0,
    writeSuccess: 0,
    writeFailure: 0
  },
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

const reporter = require('abacus-client')(statistics, errors);
const registerError = reporter.registerError;
const carryOver = require('abacus-carryover')(statistics, registerError);

const createProgressCache = () => {
  const dbConfig = {
    alias: dbalias,
    documentId: 'abacus-cf-bridge-cache'
  };
  return dbcache(dbConfig, statistics.cache);
};

const loadCFAdminToken = function *() {
  return yield retrieveToken({
    authServerURI: uris().auth_server,
    clientId: process.env.CF_CLIENT_ID,
    clientSecret: process.env.CF_CLIENT_SECRET
  });
};

const loadAbacusUsageToken = function *() {
  return yield retrieveToken({
    authServerURI: uris().auth_server,
    clientId: process.env.CLIENT_ID,
    clientSecret: process.env.CLIENT_SECRET,
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
  const progress = createProgress(createProgressCache(), lastRecordedGuid);
  yield progress.load();
  return {
    progress
  };
};

const createJob = function *(opts) {
  cfAdminToken = yield loadCFAdminToken();
  if (secured)
    linuxContainerToken = yield loadAbacusUsageToken();

  const createAppEventReader = (guid) => {
    const url = createAppEventsURL({
      afterGuid: guid
    });

    return createEventReader({
      url,
      token: cfAdminToken,
      minAge: reportingConfig.guidMinAge,
      statistics
    });
  };

  const eventFilters = [];
  if (orgsToReport)
    eventFilters.push(createOrgFilter(orgsToReport));

  const usageReporter = createUsageReporter(reporter, linuxContainerToken);

  const delayGenerator = createDelayGenerator(
    minIntervalTime,
    maxIntervalTime
  );

  const bridge = createEventBridge({
    createEventReader: createAppEventReader,
    eventFilters,
    convertEvent,
    usageReporter,
    carryOver,
    progress: opts.progress,
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
      err, undefined, perfUsageIdentifier, operationStart);
  });
  bridge.on('usage.success', (operationStart) => {
    statistics.usage.success++;
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
          config: {
            secured: secured,
            minIntervalTime: minIntervalTime,
            maxIntervalTime: maxIntervalTime,
            guidMinAge: guidMinAge,
            reporting: reportingConfig
          },
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
          statistics: statistics,
          errors: errors
        }
      }
    };
  }));
  return routes;
};

const createWeb = function *(opts) {
  const app = webapp();
  if(secured)
    app.use(/^\/v1\/cf\/applications/,
      oauth.validator(process.env.JWTKEY, process.env.JWTALGO));
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
