'use strict';

const oauth = require('abacus-oauth');
const retry = require('abacus-retry');
const urienv = require('abacus-urienv');

const debug = require('abacus-debug')('abacus-cf-services-tokens-provider');

const create = (config) => {

  const uris = urienv({
    auth_server: 9882
  });

  const systemScopes = ['abacus.usage.write', 'abacus.usage.read'];

  return {
    getStartedTokens: (cb) => {

      const cfAdminToken = oauth.cache(
        uris.auth_server, config.cfadmin.clientId, config.cfadmin.clientSecret);
      const retryStartCfAdminToken = retry(cfAdminToken.start, retry.forever);

      if(config.secured) {
        const abacusUsageToken = oauth.cache(
          uris.auth_server, config.usage.clientId, config.usage.clientSecret,
          systemScopes.join(' '));
        const retryStartAbacusUsageToken =
          retry(abacusUsageToken.start, retry.forever);

        retryStartCfAdminToken(() => {
          debug('Succesfully fetched admin token ...');
          retryStartAbacusUsageToken(() => {
            debug('Succesfully fetched usage token ...');
            cb(cfAdminToken, abacusUsageToken);
          });
        });
      }
      else
        retryStartCfAdminToken(() => {
          debug('Succesfully fetched admin token ...');
          cb(cfAdminToken);
        });
    }
  };
};

module.exports.create = create;
