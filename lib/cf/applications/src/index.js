'use strict';

const _ = require('underscore');
const memoize = _.memoize;

const moment = require('abacus-moment');
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
const runApplication = require('abacus-bridge/src/application');

const createAppEventsURL = require('./app-events-url');
const convertEvent = require('./app-event-converter');

const debug = require('abacus-debug')('abacus-cf-applications');
const edebug = require('abacus-debug')('e-abacus-cf-applications');

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

const delayGenerator = createDelayGenerator(
  minIntervalTime,
  maxIntervalTime
);

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

const progress = createProgress(createProgressCache(), lastRecordedGuid);
const saveProgress = functioncb(progress.save);
const clearProgress = functioncb(progress.clear);

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

const buildAppUsage = (event, cb) => {
  const usage = convertEvent(event);
  if (!usage) {
    cb(undefined, undefined);
    return;
  }
  // Check for usage in the same second
  carryOver.adjustTimestamp(usage, event.metadata.guid, cb);
};

const increaseRetryTimeout = (config) => {
  return delayGenerator.getNext();
};

const setReportingTimeout = (fn , interval) => {
  clearTimeout(module.usageReporter);
  module.usageReporter = setTimeout(fn, interval);
  debug('Reporting interval set to %d ms', interval);
};

const resetReportingTimeout = (fn, config) => {
  delayGenerator.reset();
  setReportingTimeout(fn, delayGenerator.getNext());
};

const reportAppUsage = (cfToken, abacusToken, { failure, success }) => {
  if (secured && !abacusToken()) {
    setReportingTimeout(() =>
      reportAppUsage(cfToken, abacusToken, {
        failure: failure,
        success: success
      }), increaseRetryTimeout(reportingConfig));
    statistics.usage.missingToken++;
    errors.missingToken = true;
    registerError('Missing resource provider token');
    failure('Missing resource provider token', null);
    return;
  }

  errors.missingToken = false;

  const orgFilter = orgsToReport ? createOrgFilter(orgsToReport) : () => false;

  const url = createAppEventsURL({
    afterGuid: progress.get().guid
  });

  const eventReader = createEventReader({
    url,
    token: cfAdminToken,
    minAge: reportingConfig.guidMinAge,
    statistics
  });
  eventReader.poll((event, done) => {
    const t0 = moment.now();
    buildAppUsage(event, (error, usage) => {
      if (error) {
        statistics.usage.loopFailures++;
        registerError('Error building usage', error, undefined,
          'usage', t0);
        done(error, undefined);
        return;
      }

      if (usage && !orgFilter(event)) {
        debug('Reporting usage event %j', usage);
        reportUsage(usage, event.metadata.guid, event.entity.state,
          abacusToken, (error, response) => {
            if (!error && response && response.statusCode === 409) {
              statistics.usage.loopConflict++;
              perf.report('usage', t0);
              done();
              return;
            }
            if (error || !response || response.statusCode !== 201) {
              statistics.usage.loopFailures++;
              registerError('Error reporting usage', error, response,
                'usage', t0);
              done(error, response);
              return;
            }

            saveProgress({
              guid: event.metadata.guid,
              timestamp: event.metadata.created_at
            }, (error) => {
              if (error) {
                done(error);
                return;
              }

              resetReportingTimeout(() => reportAppUsage(cfToken, abacusToken,
                { failure: failure, success: success }),
                reportingConfig);
              statistics.usage.loopSuccess++;
              perf.report('usage', t0);
              done();
            });
          }
        );
      }
      else {
        debug('Skipping report for usage event %j', event);
        statistics.usage.loopSkip++;
        perf.report('usage', t0);
        done();
      }
    });
  }).on('finished', (err) => {
    if (err) {
      if (err.guidNotFound) {
        edebug('CC cannot find GUID %s. Restarting reporting',
          progress.get().guid);
        clearProgress((err) => {
          edebug('Failed to clear cache!');
          setReportingTimeout(() => reportAppUsage(cfToken, abacusToken, {
            failure: failure,
            success: success
          }), increaseRetryTimeout(reportingConfig));
          failure(error, response);
        });
        return;
      }
      setReportingTimeout(() => reportAppUsage(cfToken, abacusToken, {
        failure: failure,
        success: success
      }), increaseRetryTimeout(reportingConfig));
      failure(error, response);
    }
    else {
      debug('Reporting app usage finished successfully');
      resetReportingTimeout(() => reportAppUsage(cfToken, abacusToken, {
        failure: failure,
        success: success
      }), reportingConfig);
      success();
    }
  });
};

const scheduleUsageReporting = () => {
  module.usageReporter = setTimeout(() => {
    debug('Starting usage reporting ...');
    reportAppUsage(cfAdminToken, linuxContainerToken, {
      failure: (error, response) => {},
      success: () => {}
    });
  }, reportingConfig.minInterval);
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

const load = function *() {
};

const createJob = function *() {
  yield progress.load();

  cfAdminToken = yield loadCFAdminToken();
  if (secured)
    linuxContainerToken = yield loadAbacusUsageToken();

  const start = (cb) => {
    scheduleUsageReporting();
    cb();
  };

  const stop = (cb) => {
    clearTimeout(module.usageReporter);
    cb();
  };

  return {
    start,
    stop
  };
};

const createRoutes = () => {
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
          cache: progress.get(),
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

const createWeb = function *() {
  const app = webapp();
  if(secured)
    app.use(/^\/v1\/cf\/applications/,
      oauth.validator(process.env.JWTKEY, process.env.JWTALGO));
  app.use(createRoutes());

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
