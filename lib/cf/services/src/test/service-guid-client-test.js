'use strict';

const _ = require('underscore');
const extend = _.extend;

const paging = require('abacus-paging');
const serviceGuidClient = require('../service-guid-client');

const clone = (obj) => {
  return extend({}, obj);
};

const cfAdminToken = 'cfAdminToken';
const perf = 'perf';
const statistics = 'statistics';

const createReadPageStubber = (readPageStub) => ({
  whenCalled: {
    callSuccessfulCallback : () => {
      readPageStub.callsFake((url, token, perf, statistics,
        callbacks) => {
        callbacks.success();
      });
    },
    callFailureCallback: (error) => {
      readPageStub.callsFake((url, token, perf, statistics,
        callbacks) => {
        callbacks.failure(error);
      });
    },
    callProcessResourcesCallback: (service, done) => {
      readPageStub.callsFake((url, token, perf, statistics,
        callbacks) => {
        callbacks.processResourceFn(service, done);
        callbacks.success();
      });
    }
  }
});

describe('service-bridge/service-guid-client', () => {
  let client = serviceGuidClient.create(cfAdminToken, perf, statistics);
  let readPageStubber;

  beforeEach(() => {
    sinon.stub(paging, 'readPage');
    readPageStubber = createReadPageStubber(paging.readPage);
  });

  afterEach(() => {
    paging.readPage.restore();
  });

  context('when no services are provided', () => {
    const services = {};

    beforeEach((done) => {
      readPageStubber.whenCalled.callSuccessfulCallback();

      client.injectGuids(services, (err) => {
        done();
      });
    });

    it('expect services are unchanged', () => {
      expect(services).to.deep.equal({});
    });

    it('expect paging request is not executed', () => {
      assert.notCalled(paging.readPage);
    });

  });

  context('when service is provided', () => {

    it('readPage should be invoked with proper args', (done) => {
      const services = {
        mongodb: {
          plans:[]
        },
        postgre: {
          guid: 'some-guid'
        }
      };

      readPageStubber.whenCalled.callSuccessfulCallback();

      client.injectGuids(services, (err) => {
        assert.calledOnce(paging.readPage);
        assert.calledWithExactly(paging.readPage,
          '/v2/services?q=label IN mongodb', cfAdminToken, perf, statistics,
        sinon.match.any);
        done();
      });
    });

    context('with guid', () => {
      const services = {
        mongodb: {
          guid: 'some-guid'
        }
      };
      const expectedServices = clone(services);

      beforeEach((done) => {
        readPageStubber.whenCalled.callSuccessfulCallback();

        client.injectGuids(services, (err) => {
          done();
        });
      });


      it('expect service is unchanged', () => {
        expect(services).to.deep.equal(expectedServices);
      });

      it('expect paging request is not executed', () => {
        assert.notCalled(paging.readPage);
      });
    });

    context('without guid', () => {
      let services;

      beforeEach(() => {
        services = {
          mongodb: {
            plans: []
          }
        };
      });

      context('when call is successful', () => {
        const stubbedServiceGuid = 'some-guid';
        const processResourceDoneStub = sinon.stub();

        beforeEach(() => {
          readPageStubber.whenCalled.callProcessResourcesCallback({
            entity: {
              label: 'mongodb'
            },
            metadata: {
              guid: stubbedServiceGuid
            }
          }, processResourceDoneStub);
        });

        it('expect service guid is injected', (done) => {
          client.injectGuids(services, (err) => {
            expect(services).to.deep.equal({
              mongodb: {
                plans: [],
                guid: stubbedServiceGuid
              }
            });

            assert.calledOnce(processResourceDoneStub);
            done();
          });
        });
      });

      context('when error occurs', () => {
        let injectionError;
        let actualServices;
        let expectedServices;
        const error = new Error('Read page fails');

        beforeEach((done) => {
          expectedServices = clone(services);

          readPageStubber.whenCalled.callFailureCallback(error);

          client.injectGuids(services, (err) => {
            actualServices = services;
            injectionError = err;
            done();
          });
        });

        it('expect error is propagated', () => {
          expect(injectionError).to.deep.equal(error);
        });

        it('expect service is unchanged', () => {
          expect(actualServices).to.deep.equal(expectedServices);
        });

      });
    });
  });
});
