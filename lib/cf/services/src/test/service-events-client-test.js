'use strict';

const moment = require('abacus-moment');
const paging = require('abacus-paging');
const serviceEventsClient = require('../service-events-client');

describe('service-bridge/service-events-client', () => {
  const sandbox = sinon.sandbox.create();
  const perf = 'fake-performance-object';
  const statistics = 'fake-statistics-object';
  const cfAdminToken = 'fake-cf-admin-token-provider';
  const documentCreationTime = 100000;
  const testOrg = 'org-guid';

  const document = {
    metadata: {
      created_at: documentCreationTime,
      guid: 'some-guid'
    },
    entity: {
      org_guid: testOrg
    }
  };
  const minAge = 2000;

  let readPageStub;
  let processStub;
  let successedStub;
  let failedStub;

  beforeEach(() => {
    processStub = sandbox.stub();
    successedStub = sandbox.stub();
    failedStub = sandbox.stub();
    readPageStub = sandbox.stub(paging, 'readPage');
  });

  afterEach(() => {
    sandbox.restore();
  });

  const startRetrieving = ({ serviceGuids, afterGuid, orgsToReport } = {}) => {
    const client = serviceEventsClient.create({
      cfAdminToken,
      perf,
      statistics,
      minAge,
      orgsToReport
    });

    const retriever = client.retriever({
      serviceGuids,
      afterGuid
    });

    retriever.forEachEvent(processStub);
    retriever.whenSuccessed(successedStub);
    retriever.whenFailed(failedStub);
    retriever.start();
  };

  describe('verify query building', () => {

    context('when default config is used', () => {
      beforeEach(() => {
        startRetrieving();
      });

      it('calls readPage with proper arguments', () => {
        assert.calledOnce(paging.readPage);
        assert.calledWithExactly(paging.readPage,
          sinon.match('/v2/service_usage_events?')
          .and(sinon.match('order-direction=asc'))
          .and(sinon.match('results-per-page=50'))
          .and(sinon.match(
            'q=service_instance_type%3Amanaged_service_instance')),
          cfAdminToken,
          perf,
          statistics,
          sinon.match.any);
      });
    });

    context('when service guids are provided', () => {
      const firstGuid = 'first-guid';
      const secondGuid = 'second-guid';

      beforeEach(() => {
        startRetrieving({
          serviceGuids: [firstGuid, secondGuid]
        });
      });

      it('calls readPage with proper arguments', () => {
        assert.calledOnce(paging.readPage);
        assert.calledWithExactly(paging.readPage,
          sinon.match('/v2/service_usage_events?')
          .and(sinon.match('order-direction=asc'))
          .and(sinon.match('results-per-page=50'))
          .and(sinon.match(
            'q=service_instance_type%3Amanaged_service_instance'))
          .and(sinon.match(
            `q=service_guid%20IN%20${firstGuid}%2C${secondGuid}`)),
          cfAdminToken,
          perf,
          statistics,
          sinon.match.any);
      });
    });

    context('when after guid is provided', () => {
      const afterGuid = 'after-this-guid';

      beforeEach(() => {
        startRetrieving({
          afterGuid: afterGuid
        });
      });

      it('calls readPage with proper arguments', () => {
        assert.calledOnce(paging.readPage);
        assert.calledWithExactly(paging.readPage,
          sinon.match('/v2/service_usage_events?')
          .and(sinon.match('order-direction=asc'))
          .and(sinon.match('results-per-page=50'))
          .and(sinon.match(
            'q=service_instance_type%3Amanaged_service_instance'))
          .and(sinon.match(`after_guid=${afterGuid}`)),
          cfAdminToken,
          perf,
          statistics,
          sinon.match.any);
      });
    });
  });

  describe('verify events processing finished', () => {

    context('when readPage reports failure', () => {
      const failureErr = new Error('failed!');
      const expectedErrorMessage =
        'Could not process events due to "Error: failed!":"undefined"';

      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
            processResourceFn, success, failure
          }) => {
          failure(failureErr);
        });
        startRetrieving();
      });

      it('propagates arguments to the client "whenFailed" callback', () => {
        assert.calledOnce(failedStub);
        assert.calledWithExactly(failedStub, sinon.match.instanceOf(Error));
        assert.calledWith(failedStub, sinon.match((err) => {
          return err.message == expectedErrorMessage;
        }));
      });
    });

    context('when readPage reports failure without error object', () => {
      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
            processResourceFn, success, failure
          }) => {
          failure();
        });
        startRetrieving();
      });

      it('propagates arguments to the client "whenFailed" callback', () => {
        assert.calledOnce(failedStub);
        assert.calledWithExactly(failedStub, sinon.match.instanceOf(Error));
      });
    });

    context('when readPage hit "guid not found" specific error', () => {
      const failureErr = new Error('failed!');
      const response = {
        statusCode: 400,
        body: {
          code: 10005
        }
      };

      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
            processResourceFn, success, failure
          }) => {
          failure(failureErr, response);
        });
        startRetrieving();
      });

      it('propagates the specific error', () => {
        assert.calledOnce(failedStub);
        assert.calledWithExactly(failedStub,
          serviceEventsClient.guidNotFoundError);
      });
    });

    context('when readPage reports success', () => {
      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
            processResourceFn, success, failure
          }) => {
          success();
        });
        startRetrieving();
      });

      it('propagates arguments to the client "whenSuccessed" callback', () => {
        assert.calledOnce(successedStub);
        assert.calledWithExactly(successedStub);
      });
    });

    context('when retriever finished callbacks are not provided', () => {
      let retriever;

      const retrieve = (cb) => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
          processResourceFn, success, failure
        }) => {
          cb(success, failure);
        });

        const client = serviceEventsClient.create(cfAdminToken,
          perf, statistics);
        retriever = client.retriever({});
      };

      context('when "whenSuccessed" is not called', () => {

        beforeEach(() => {
          retrieve((success, failure) => success());
          retriever.whenFailed(() => {});
        });

        it('execution does not fail', () => {
          retriever.start();
        });
      });

      context('when "whenFailed" is not called', () => {

        beforeEach(() => {
          retrieve((success, failure) => failure());
          retriever.whenSuccessed(() => {});
        });

        it('execution does not fail', () => {
          retriever.start();
        });
      });
    });

  });

  describe('verify events processing', () => {

    const someCallback = sandbox.stub();

    beforeEach(() => {
      readPageStub.callsFake((uri, cfToken, perf, statistics, {
        processResourceFn, success, failure
      }) => {
        processResourceFn(document, someCallback);
      });
    });

    context('when forEachEvent is called', () => {

      context('when reporting for all orgs', () => {

        beforeEach(() => {
          stub(moment, 'now');
        });

        afterEach(() => {
          moment.now.restore();
        });

        context('when event is old enough', () => {

          beforeEach(() => {
            moment.now.returns(documentCreationTime + minAge + 1);
            startRetrieving();
          });

          it('propagates arguments to provided callback', () => {
            assert.calledOnce(processStub);
            assert.calledWithExactly(processStub, document, someCallback);
          });
        });

        context('when event is not old enough', () => {

          beforeEach(() => {
            moment.now.returns(documentCreationTime + minAge - 1);
            startRetrieving();
          });

          it('callback is never called', () => {
            assert.notCalled(processStub);
          });

        });
      });

      context('when event\'s org is not in the enabled orgs', () => {

        beforeEach(() => {
          startRetrieving({ orgsToReport: ['nonreportable-org-guid'] });
        });

        it('callback is never called', () => {
          assert.notCalled(processStub);
        });

      });

      context('when event\'s org is in the enabled orgs', () => {

        beforeEach(() => {
          startRetrieving({ orgsToReport: [testOrg] });
        });

        it('propagates arguments to provided callback', () => {
          assert.calledOnce(processStub);
          assert.calledWithExactly(processStub, document, someCallback);
        });

      });

    });

    context('when forEachEvent is NOT called', () => {

      beforeEach(() => {
        const client = serviceEventsClient.create({
          cfAdminToken,
          perf,
          statistics,
          minAge
        });
        const retriever = client.retriever({});
        retriever.start();
      });

      it('should execute default callback', () => {
        assert.calledOnce(someCallback);
      });
    });

  });
});
