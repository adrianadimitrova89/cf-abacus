'use strict';

// Usage collector service, responsible for receiving usage data batches
// submitted by service providers and storing them in the usage db.

const _ = require('underscore');
const map = _.map;
const extend = _.extend;
const pick = _.pick;
const find = _.find;

const tracing = require('abacus-tracing');
const webapp = require('abacus-webapp');
const router = require('abacus-router');
const batch = require('abacus-batch');
const retry = require('abacus-retry');
const breaker = require('abacus-breaker');
const cluster = require('abacus-cluster');
const dataflow = require('abacus-dataflow');
const lru = require('abacus-lrucache');
const mconfig = require('abacus-metering-config');
const oauth = require('abacus-oauth');
const pconfig = require('abacus-pricing-config');
const request = require('abacus-request');
const rconfig = require('abacus-rating-config');
const schemas = require('abacus-usage-schemas');
const seqid = require('abacus-seqid');
const urienv = require('abacus-urienv');
const yieldable = require('abacus-yieldable');

const brequest = yieldable(retry(breaker(batch(request))));

// Setup debug log
const debug = require('abacus-debug')('abacus-usage-collector');
const edebug = require('abacus-debug')('e-abacus-usage-collector');

// Secure the routes or not
const secured = () => process.env.SECURED === 'true' ? true : false;

// OAuth bearer access token with Abacus system access scopes
let systemToken;

// Resolve service URIs
const uris = urienv({
  provisioning: 9880,
  auth_server: 9882,
  meter: 9100,
  account: 9881
});

// Return OAuth resource or system scopes needed to write input docs
const iwscope = (udoc) => secured() ? {
  resource: [['abacus.usage', udoc.resource_id, 'write'].join('.')],
  system: ['abacus.usage.write']
} : undefined;

// Return OAuth resource or system scopes needed to read input docs
const irscope = (udoc) => secured() ? {
  resource: [['abacus.usage', udoc.resource_id, 'read'].join('.')],
  system: ['abacus.usage.read']
} : undefined;

// Return OAuth resource or system scopes needed to read output docs
const orscope = (udoc) => secured() ? {
  system: ['abacus.usage.read']
} : undefined;

// Return Oauth resource or system scopes needed to read error docs
const erscope = (udoc) => secured() ? {
  system: ['abacus.usage.read']
} : undefined;

const edscope = (udoc) => secured() ? {
  system: ['abacus.usage.write']
} : undefined;

// Return the keys and times of our docs
const ikey = (udoc, auth) => {
  let userInfo;
  if (secured() && auth)
    // Get user information from authorization token
    userInfo = oauth.getUserInfo(auth.replace(/^bearer /i, ''));

  // Use client id or anonymous
  return userInfo && userInfo.client_id || 'anonymous';
};

const itime = (udoc) =>
  seqid();

const okeys = (udoc, ikey) =>
  [[ikey, udoc.resource_id, udoc.plan_id,
    udoc.organization_id, udoc.space_id, udoc.resource_instance_id,
    udoc.consumer_id || 'UNKNOWN'].join('/')];

const otimes = (udoc, itime) =>
  [map([udoc.end, udoc.start], seqid.pad16).concat([itime]).join('/')];

const etime = (udoc, itime) =>
  map([udoc.end, udoc.start], seqid.pad16).concat([itime]).join('/');

const ekey = (udoc, ikey) =>
  [ikey, udoc.resource_id, udoc.plan_id,
    udoc.organization_id, udoc.space_id, udoc.resource_instance_id,
    udoc.consumer_id || 'UNKNOWN'].join('/');

// Business response object
const error = (err, reason, cause) => ({
  error: err,
  reason: reason,
  cause: cause
});

// Retrieve resource type given the resource id, and attach it to u
const getResourceType = function *(rid, auth) {
  debug('Retrieving resource type for resource id %s', rid);

  const o = auth ? { headers: { authorization: auth } } : {};
  // Get resource type
  const res = yield brequest.get(uris.provisioning +
    '/v1/provisioning/resources/:resource_id/type',
  extend({}, o, {
    cache: true,
    resource_id: rid
  }));

  // Unable to retrieve resource type
  if (res.statusCode !== 200) {
    edebug('Unable to retrieve resource type, %o', res);
    debug('Unable to retrieve resource type, %o', res);

    // Return with the reason of the error
    return error('ertnotfound',
      'Unable to retrieve resource type for resource id ' +
      rid, res);
  }

  // Return the resource_type
  return {
    resource_type: res.body
  };
};

// Maintain a cache of accounts
const accounts = lru({
  max: 10000,
  maxAge: 1000 * 60 * 20
});

const cachedAccount = (k) => accounts.get(k);

const cacheAccount = (k, account) => {
  accounts.set(k, account);
  return account;
};

// Return the account_id and pricing_country
const getAccount = function *(oid, time, auth) {
  debug('Retrieving account information for org %s', oid);

  // Round time to a 10 min boundary
  const t = Math.floor(time / 600000) * 600000;
  const k = [oid, t].join('/');

  // Look in our cache first
  const ac = cachedAccount(k);
  if (ac) {
    debug('Accont information found in the cache');
    return {
      account: ac
    };
  }

  const o = auth ? { headers: { authorization: auth } } : {};

  // Get account information
  const res = yield brequest.get(uris.account +
    '/v1/organizations/:org_id/account/:time',
  extend({}, o, {
    org_id: oid,
    time: time
  }));

  // Unable to retrieve account information
  if (res.statusCode !== 200) {
    edebug('Unable to retrieve account information, %o', res);
    debug('Unable to retrieve account information, %o', res);

    // Return with the reason of the error
    return error('eorgnotfound',
      'Unable to retrieve account info for ' + oid + ' at ' +
      time, res);
  }

  // cache and returns Return the account_id and the pricing_country
  return {
    account: cacheAccount(k, pick(res.body, 'account_id', 'pricing_country'))
  };
};

// Return the metering_plan_id
const meteringId = yieldable(mconfig.id);

// Return the rating_plan_id
const ratingId = yieldable(rconfig.id);

// Return the prices and the pricing_plan_id
const pricingId = yieldable(pconfig.id);

// Return the pricing plan
const pricingPlan = yieldable(pconfig);

// find info with error and reason to redirect 
// usage to error db and stop processing it to the next pipeline.
const findError = (info) => find(info, (i) => i.error);

// Map submitted usage doc to normalized usage doc
/* eslint complexity: [1, 8] */
const normalizeUsage = function *(udoc, auth, ctx) {
  debug('Normalizing usage %o', udoc);

  return yield tracing.runSpan(ctx, 'normalize-usage', function *(
    normCtx, normSpan) {

    normSpan.setTag('organization-id', udoc.organization_id);
    normSpan.setTag('space-id', udoc.space_id);
    normSpan.setTag('consumer-id', udoc.consumer_id || 'UNKNOWN');
    normSpan.setTag('resource-id', udoc.resource_id);
    normSpan.setTag('plan-id', udoc.plan_id);
    normSpan.setTag('resource-instance-id', udoc.resource_instance_id);
    normSpan.setTag('time', udoc.end);

    // Forward authorization header field to provisioning
    const o = auth ? { headers: { authorization: auth } } : {};
  
    yield tracing.runSpan(normCtx, 'validate-usage', function *() {
      const res = yield brequest.get(uris.provisioning +
        '/v1/provisioning/organizations/:organization_id/spaces/' +
        ':space_id/consumers/:consumer_id/resources/:resource_id/plans/' +
        ':plan_id/instances/:resource_instance_id/:time',
      extend({}, o, udoc, {
        consumer_id: udoc.consumer_id || 'UNKNOWN',
        time: udoc.end
      }));
    
      if (res.statusCode !== 200) {
        const sanitizedResponse = pick(res, 'statusCode', 'headers', 'body');
        edebug('Usage validation failed, %o', sanitizedResponse);
        debug('Usage validation failed, %o', sanitizedResponse);
        throw sanitizedResponse;
      }
    });
  
    // Retrieve resource type given the resource id
    const [rt, account] = yield [
      tracing.runSpan(normCtx, 'get-resource-type', function *() {
        return yield getResourceType(udoc.resource_id, auth);
      }),
      tracing.runSpan(normCtx, 'get-account', function *() {
        return yield getAccount(udoc.organization_id, udoc.end, auth);
      })
    ];
    let e = findError([rt, account]);
  
    // Stop when resource type or account returns undefined.
    if(e) {
      debug('The usage submitted has business errors %o', e);
      return [extend({}, udoc, e)];
    }
  
    // Get account information and plan ids
    const [mpid, rpid, ppid] = yield [
      tracing.runSpan(normCtx, 'get-metering-id', function *() {
        return yield meteringId(
          udoc.organization_id, rt.resource_type, udoc.plan_id, udoc.end, auth);
      }),
      tracing.runSpan(normCtx, 'get-rating-id', function *() {
        return yield ratingId(
          udoc.organization_id, rt.resource_type, udoc.plan_id, udoc.end, auth);
      }),
      tracing.runSpan(normCtx, 'get-pricing-id', function *() {
        return yield pricingId(
          udoc.organization_id, rt.resource_type, udoc.plan_id, udoc.end, auth);
      })
    ];
    e = findError([mpid, rpid, ppid]);
    if(e) {
      debug('The usage submitted has business errors %o', e);
      return [extend({}, udoc, e)];
    }

    const prices = yield tracing.runSpan(normCtx, 'get-prices', function *() {
      return yield pricingPlan(ppid.pricing_plan_id,
        !account.error && account.account.pricing_country, auth);
    });  
    e = findError([prices]);  
    if(e) {
      debug('The usage submitted has business errors %o', e);
      return [extend({}, udoc, e)];
    }
  
    return [extend({}, udoc, {
      resource_type: rt.resource_type,
      account_id: account.account.account_id,
      pricing_country: account.account.pricing_country,
      metering_plan_id: mpid.metering_plan_id,
      rating_plan_id: rpid.rating_plan_id,
      pricing_plan_id: ppid.pricing_plan_id,
      prices: prices.pricing_plan
    })];
  });
};

// Create a collector service app
const collector = () => {
  // Create the Webapp
  const app = webapp();
  app.use(tracing.middleware);

  // Secure metering and batch routes using an OAuth bearer access token
  if (secured())
    app.use(/^\/v1\/metering|^\/batch$/,
      oauth.validator(process.env.JWTKEY, process.env.JWTALGO));

  const mapper = dataflow.mapper(normalizeUsage, {
    input: {
      type: 'collected_usage',
      schema: schemas.resourceUsage,
      post: '/v1/metering/collected/usage',
      get: '/v1/metering/collected/usage/t/:tseq/k/:kprovider',
      dbname: 'abacus-collector-collected-usage',
      wscope: iwscope,
      rscope: irscope,
      key: ikey,
      time: itime,
      dedupe: false,
      authentication: systemToken
    },
    output: {
      type: 'normalized_usage',
      get: '/v1/metering/normalized/usage' + 
        '/k/:kprovider/:kresource_id/:kplan_id' +
        '/:korganization_id/:kspace_id' +
        '/:kresource_instance_id/:kconsumer_id' +
        '/t/:tend/:tstart/:tseq',
      dbname: 'abacus-collector-normalized-usage',
      rscope: orscope,
      keys: okeys,
      times: otimes
    },
    error: {
      type: 'collected_error',
      get: '/v1/metering/collected/usage/t/:tstart/:tend/get/error',
      delete: '/v1/metering/collected/usage/t/:tend/:tstart/:tseq' +
        '/k/:kprovider/:kresource_id/:kplan_id' +
        '/:korganization_id/:kspace_id/:kresource_instance_id' +
        '/:kconsumer_id/delete/error',
      dbname: 'abacus-collector-error-usage',
      rscope: erscope,
      dscope: edscope,
      key: ekey,
      time: etime
    },
    sink: {
      host: uris.meter,
      apps: process.env.METER_APPS,
      posts: ['/v1/metering/normalized/usage'],
      authentication: systemToken
    }
  });

  app.use(mapper);
  app.use(router.batch(app));

  app.mapper = mapper;
  return app;
};

const startApp = (err) => {
  // Create the app
  const app = collector();
  app.listen();

  // Replay any failed events on master
  if (!cluster.isWorker()) {
    const replayDelay = err ? 20000 : 0;
    debug('Starting replay in %d ms ...', replayDelay);
    setTimeout(() => dataflow.replay(app.mapper, 0, (err) => {
      debug('Replay completed %s', err ? 'with error' : 'successfully');
    }), replayDelay);
  }
};

const runCLI = () => {
  if (secured()) {
    // Cache and schedule the system token renewal
    systemToken = oauth.cache(uris.auth_server, process.env.CLIENT_ID,
      process.env.CLIENT_SECRET, 'abacus.usage.write abacus.usage.read');

    systemToken.start(startApp);
  }
  else
    startApp();
};

// Export our public functions
module.exports = collector;
module.exports.normalize = normalizeUsage;
module.exports.account = getAccount;
module.exports.resourceType = getResourceType;
module.exports.runCLI = runCLI;
