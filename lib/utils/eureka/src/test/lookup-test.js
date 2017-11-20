'use strict';

const sandbox = sinon.sandbox.create();

const tests = (secured) => {

  let eureka;

  before(() => {
    process.env.SECURED = secured ? 'true' : 'false';
    process.env.EUREKA_USER = 'user';
    process.env.EUREKA_PASSWORD = 'password';

    require('abacus-request');
    eureka = require('..');
  });

  after(() => {
    delete process.env.SECURED;
    delete process.env.EUREKA_USER;
    delete process.env.EUREKA_PASSWORD;

    delete require.cache[require.resolve('abacus-request')];
    delete require.cache[require.resolve('..')];
  });

  afterEach(() => {
    sandbox.restore();
  });

  describe('lookup', () => {

    let getStub;

    beforeEach(() => {
      const okResponse = {
        statusCode: 200,
        body: {
          instance: {
            app: 'TEST',
            hostName: 'test-0.0',
            ipAddr: '127.0.0.1',
            port: {
              $: 1234
            }
          }
        }
      };

      getStub = sandbox.stub().yields(undefined, okResponse);
      require.cache[require.resolve('abacus-request')].exports.get =
        getStub;
    });

    it('gets correct instance data', (done) => {
      eureka.instance('http://test.com', 'test', '0', '0', (err, val) => {
        expect(err).to.equal(undefined);
        expect(val).to.deep.equal({
          address: '127.0.0.1',
          app: 'TEST',
          instance: 'test-0.0',
          port: 1234
        });

        done();
      });
    });
  });
};

describe('abacus-eureka unsecured', () => tests(false));
describe('abacus-eureka secured', () => tests(true));
