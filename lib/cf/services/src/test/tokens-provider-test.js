'use strict';
/* eslint no-unused-expressions: 0 */

const oauth = require('abacus-oauth');
const stubbedmodule = require('./stubber');

const urienvModuleStub = stubbedmodule('abacus-urienv');

const tokensProviderCreator = require('../tokens-provider');

describe('service-bridge/tokens-provider', () => {
  const sandbox = sinon.sandbox.create();

  const adminClientId = 'cfAdminClient';
  const adminClientSecret = 'cfAdminSecret';
  const usageClientId = 'abacusUsageClient';
  const usageClientSecret = 'abacusUsageSecret';
  const authServer = 'https://localhost:1234';

  let cfAdminTokenStub;
  let abacusUsageTokenStub;
  let oauthCacheStub;

  let tokensProvider;
  let returnedCfAdminToken;
  let returnedAbacusUsageToken;

  beforeEach(() => {

    oauthCacheStub = sandbox.stub(oauth, 'cache');

    const uriEnvStub = sandbox.stub();
    uriEnvStub.withArgs({
      auth_server: 9882
    }).returns({
      auth_server: authServer
    });
    urienvModuleStub.stubMainFunc(uriEnvStub);

  });

  afterEach(() => {
    sandbox.restore();
  });

  context('when secured', () => {

    const execute = (tokenStubberFn, cb) => {
      cfAdminTokenStub = tokenStubberFn();
      abacusUsageTokenStub = tokenStubberFn();
      const systemScopes = 'abacus.usage.write abacus.usage.read';

      oauthCacheStub.withArgs(authServer, adminClientId, adminClientSecret)
        .returns(cfAdminTokenStub);
      oauthCacheStub
        .withArgs(authServer, usageClientId, usageClientSecret, systemScopes)
        .returns(abacusUsageTokenStub);

      const config = {
        cfadmin: {
          clientId: adminClientId,
          clientSecret: adminClientSecret
        },
        usage: {
          clientId: usageClientId,
          clientSecret: usageClientSecret
        },
        secured: true
      };
      tokensProvider = tokensProviderCreator.create(config);

      tokensProvider.getStartedTokens((cfAdminToken, abacusUsageToken) => {
        cb({ cfAdminToken, abacusUsageToken });
      });

    };

    context('when requested', () => {

      beforeEach((done) => {

        const createTokenStub = () => ({
          start: sandbox.stub().yields()
        });

        execute(createTokenStub, (tokens) => {
          returnedCfAdminToken = tokens.cfAdminToken;
          returnedAbacusUsageToken = tokens.abacusUsageToken;
          done();
        });
      });

      it('cf admin token is returned', () => {
        expect(returnedCfAdminToken).to.equal(cfAdminTokenStub);
      });

      it('abacus usage token is returned', () => {
        expect(returnedAbacusUsageToken).to.equal(abacusUsageTokenStub);
      });

      it('cf admin token is started', () => {
        assert.calledOnce(returnedCfAdminToken.start);
      });

      it('abacus usage token is started', () => {
        assert.calledOnce(returnedAbacusUsageToken.start);
      });

    });

    context('when admin token start fails', () => {

      beforeEach((done) => {

        const createTokenStub = () => ({
          start: sandbox.stub()
                  .onFirstCall().yields('some err')
                  .onSecondCall().yields()
        });

        execute(createTokenStub, (tokens) => {
          returnedCfAdminToken = tokens.cfAdminToken;
          returnedAbacusUsageToken = tokens.abacusUsageToken;
          done();
        });
      });

      it('cf admin token start is retried', () => {
        assert.calledTwice(returnedCfAdminToken.start);
      });

      it('abacus usage token start is retried', () => {
        assert.calledTwice(returnedAbacusUsageToken.start);
      });

    });
  });

  context('when not secured', () => {

    const execute = (tokenStubberFn, cb) => {
      cfAdminTokenStub = tokenStubberFn();

      oauthCacheStub.withArgs(authServer, adminClientId, adminClientSecret)
        .returns(cfAdminTokenStub);

      const config = {
        cfadmin: {
          clientId: adminClientId,
          clientSecret: adminClientSecret
        },
        secured: false
      };

      tokensProvider = tokensProviderCreator.create(config);

      tokensProvider.getStartedTokens((cfAdminToken, abacusUsageToken) => {
        cb({ cfAdminToken, abacusUsageToken });
      });

    };

    context('when requested', () => {

      beforeEach((done) => {

        const createTokenStub = () => ({
          start: sandbox.stub().yields()
        });

        execute(createTokenStub, (tokens) => {
          returnedCfAdminToken = tokens.cfAdminToken;
          returnedAbacusUsageToken = tokens.abacusUsageToken;
          done();
        });
      });

      it('cf admin token is returned', () => {
        expect(returnedCfAdminToken).to.equal(cfAdminTokenStub);
      });

      it('abacus usage token to be undefined', () => {
        expect(returnedAbacusUsageToken).to.be.undefined;
      });

      it('cf admin token is started', () => {
        assert.calledOnce(returnedCfAdminToken.start);
      });

    });

    context('when admin token start fails', () => {

      beforeEach((done) => {

        const createTokenStub = () => ({
          start: sandbox.stub()
                  .onFirstCall().yields('some err')
                  .onSecondCall().yields()
        });

        execute(createTokenStub, (tokens) => {
          returnedCfAdminToken = tokens.cfAdminToken;
          done();
        });
      });

      it('cf admin token start is retried', () => {
        assert.calledTwice(returnedCfAdminToken.start);
      });

    });

  });



});
