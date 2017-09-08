'use strict';

const cluster = require('abacus-cluster');
const moment = require('abacus-moment');
const oauth = require('abacus-oauth');
const perf = require('abacus-perf');
const router = require('abacus-router');
const throttle = require('abacus-throttle');
const urienv = require('abacus-urienv');
const webapp = require('abacus-webapp');
const paging = require('abacus-paging');
const serviceGuidClient = require('./service-guid-client');
const config = require('./config');
const dbcache = require('./dbcache');
const _ = require('underscore');
const extend = _.extend;
const memoize = _.memoize;
// const filter = _.filter;

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
    maxRetries     : Math.floor(
      Math.log(maxIntervalTime - minIntervalTime + 1)),
    orgsToReport   : orgsToReport
  };
})();


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

// Initialize reporter with statistics, errors and get errors function
const reporter = require('abacus-client')(statistics, errors);
const registerError = reporter.registerError;

// Initialize carryOver with statistics and error function
const carryOver = require('abacus-carryover')(statistics, registerError);


// SERVICE GUID CACHE CODE HERE ............................

const serviceGuidsCache = {
  guids: [],
  isEmpty: function() {
    return this.guids.length == 0;
  }
};

const isServiceGuidConfigComplete = (guidsToCheck) =>
  guidsToCheck.length == Object.keys(cfg.services).length;

const serviceGuids = () => {
  const guids = [];

  if (!serviceGuidsCache.isEmpty())
    return serviceGuidsCache.guids;

  for (let service in cfg.services)
    if (cfg.services[service].guid)
      guids.push(cfg.services[service].guid);
  if (isServiceGuidConfigComplete(guids))
    serviceGuidsCache.guids = guids;

  return guids;
};

// LAST KNOWN EVENT CACHE CODE HERE ............................

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

const reportPlanUsage = (serviceLabel, planName) => {
  if (cfg.services && cfg.services[serviceLabel])
    return cfg.services[serviceLabel].plans.includes(planName);
  return false;
};

const supportedState = (state) => {
  switch (state) {
    case 'CREATED':
    case 'DELETED':
      return true;
    default:
      debug('Unsupported service event state %s', state);
      return false;
  }
};

const translateEventToUsage = (state) => [
  {
    measure: 'current_instances',
    quantity: state === 'CREATED' ? 1 : 0
  },
  {
    measure: 'previous_instances',
    quantity: state === 'CREATED' ? 0 : 1
  }
];

const buildServiceUsage = (event, cb) => {
  const serviceLabel = event.entity.service_label;
  const planName = event.entity.service_plan_name;

  if (!serviceLabel || !supportedState(event.entity.state) ||
      !reportPlanUsage(serviceLabel, planName)) {
    cb();
    return;
  }

  const eventTime = moment.utc(event.metadata.created_at).valueOf();
  const serviceGUID = `service:${event.entity.service_instance_guid}`;

  const usageDoc = {
    start: eventTime,
    end: eventTime,
    organization_id: event.entity.org_guid,
    space_id: event.entity.space_guid,
    consumer_id: serviceGUID,
    resource_id: serviceLabel,
    plan_id: planName,
    resource_instance_id: `${serviceGUID}:${planName}:${serviceLabel}`,
    measured_usage: translateEventToUsage(event.entity.state)
  };

  // Check for usage in the same second
  carryOver.adjustTimestamp(usageDoc, event.metadata.guid, cb);
};

/**
 * "The list of usage events returned from the API is not guaranteed to be
 * complete. Events may still be processing at the time of the query, so
 * events that occurred before the final event may still appear
 * [...]
 * it is recommended that operators select their ‘after_guid’ from an event
 * far enough back in time to ensure that all events have been processed"
 *
 * https://www.cloudfoundry.org/how-to-bill-on-cloud-foundry/
 */
const isElder = (resource) => {
  const now = moment.now();
  const resourceDate = moment.utc(resource.metadata.created_at).valueOf();
  const age = now - resourceDate;
  const elder = age > cfg.polling.events.minAge;
  debug('Resource %s has age %d. Minimum resource age is %d. Elder: %s',
    resource.metadata.guid, age, cfg.polling.events.minAge, elder);

  return elder;
};

const isOrgEnabled = (resource) => cfg.polling.orgs ?
  cfg.polling.orgs.includes(resource.entity.org_guid) : true;

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

// RETRY LOGIC HERE .............................

let currentRetries = 0;

const getRetryTimeout = (config) => {
  return config.minInterval + Math.floor(Math.expm1(currentRetries));
};

const increaseRetryTimeout = (config) => {
  currentRetries++;
  let interval = config.maxInterval;
  if (currentRetries <= config.maxRetries)
    interval = getRetryTimeout(config);
  return interval;
};

const setReportingTimeout = (fn , interval) => {
  clearTimeout(module.usageReporter);
  module.usageReporter = setTimeout(fn, interval);
  debug('Reporting interval set to %d ms', interval);
};

const resetReportingTimeout = (fn, config) => {
  currentRetries = 0;
  setReportingTimeout(fn, config.minInterval);
};

// UP TO HERE .............................


const reportServiceUsage = (cfToken, abacusToken, { failure, success }) => {
  if (cfg.oauth.enabled && !abacusToken()) {
    setReportingTimeout(() =>
      reportServiceUsage(cfToken, abacusToken, {
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
  let uri = '/v2/service_usage_events?order-direction=asc&results-per-page=50' +
  '&q=service_instance_type:managed_service_instance';
  if (cfg.services)
    uri += '&q=service_guid IN ' + serviceGuids().join(',');
  if (lastRecordedDoc.lastRecordedGUID)
    uri += '&after_guid=' + lastRecordedDoc.lastRecordedGUID;

  paging.readPage(uri, cfToken, perf, statistics, {
    processResourceFn: (event, done) => {
      const t0 = moment.now();

      buildServiceUsage(event, (error, usage) => {
        if (error) {
          statistics.usage.loopFailures++;
          registerError('Error building usage', error, undefined,
            'usage', t0);
          done(error);
          return;
        }

        if (usage && isElder(event) && isOrgEnabled(event)) {
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

              storeGuid(event);
              updateLastRecordedDoc((error) => {
                if (error) {
                  done(error);
                  return;
                }

                resetReportingTimeout(() => reportServiceUsage(
                  cfToken, abacusToken, { failure: failure, success: success }),
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
    },
    failure: (error, response) => {
      if (response && response.statusCode === 400 &&
        response.body && response.body.code === 10005) {
        edebug('CC cannot find GUID %s. Restarting reporting',
          lastRecordedDoc.lastRecordedGUID);
        lastRecordedDoc.lastRecordedGUID = undefined;
        lastRecordedDoc.lastRecordedTimestamp = undefined;
      }

      setReportingTimeout(() => reportServiceUsage(cfToken, abacusToken, {
        failure: failure,
        success: success
      }), increaseRetryTimeout(reportingConfig));
      failure(error, response);
    },
    success: () => {
      debug('Reporting service usage finished successfully');
      resetReportingTimeout(() => reportServiceUsage(cfToken, abacusToken, {
        failure: failure,
        success: success
      }), reportingConfig);
      success();
    }
  });
};

const stopReporting = (cb = () => {}) => {
  edebug('Cancelling timers');
  clearTimeout(module.usageReporter);
  cb();
};

const startUsageReporting = () => {
  module.usageReporter = setTimeout(() => {
    debug('Starting usage reporting ...');
    reportServiceUsage(cfAdminToken, serviceUsageToken, {
      failure: (error, response) => {},
      success: () => {}
    });
  }, reportingConfig.minInterval);
};

const loadServiceGuidsConfiguration = (retryFn, cb) => {
  const client = serviceGuidClient.create(cfAdminToken, perf, statistics);
  client.injectGuids(cfg.services, (err) => {
    if (err) {
      setReportingTimeout(retryFn, increaseRetryTimeout(reportingConfig));
      return;
    }

    if (serviceGuids().length === 0) {
      edebug('Not able to read service guids, retrying...');
      setReportingTimeout(retryFn, increaseRetryTimeout(reportingConfig));
      return;
    }

    cb();
  });
};

const scheduleUsageReporting = () => {
  cfAdminToken.start((err) => {
    if (err) {
      edebug('Unable to get CF admin token due to %o, retrying...', err);
      setReportingTimeout(scheduleUsageReporting,
        increaseRetryTimeout(reportingConfig));
      return;
    }
    debug('Succesfully fetched token, so schedule service bridge reporting...');
    if (isServiceGuidConfigComplete(serviceGuids()))
      startUsageReporting();
    else
      loadServiceGuidsConfiguration(scheduleUsageReporting, () => {
        startUsageReporting();
      });
  });

  if (cfg.oauth.enabled)
    serviceUsageToken.start();

  process.on('exit', () => {
    stopReporting();
  });
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
      scheduleUsageReporting();
    });
  }

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
