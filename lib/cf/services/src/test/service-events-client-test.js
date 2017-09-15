'use strict';

const paging = require('abacus-paging');
const serviceEventsClient = require('../service-events-client');

describe('service-bridge/service-events-client', () => {
  const sandbox = sinon.sandbox.create();
  const perf = 'fake-performance-object';
  const statistics = 'fake-statistics-object';
  const cfAdminToken = 'fake-cf-admin-token-provider';
  const document = 'fake-document';

  let readPageStub;
  let processStub;
  let finishedStub;

  beforeEach(() => {
    processStub = sandbox.stub();
    finishedStub = sandbox.stub();
    readPageStub = sandbox.stub(paging, 'readPage');
  });

  afterEach(() => {
    sandbox.restore();
  });

  const runGetAll = ({ serviceGuids, afterGuid } = {}) => {
    const client = serviceEventsClient.create(cfAdminToken, perf, statistics);
    const retriever = client.retriever({
      serviceGuids,
      afterGuid
    });

    retriever.forEachEvent(processStub);
    retriever.whenFinished(finishedStub);
    retriever.start();
  };

  describe('verify query building', () => {

    context('when default config is used', () => {
      beforeEach(() => {
        runGetAll();
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
        runGetAll({
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
        runGetAll({
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

  describe('verify events processing finishd', () => {
    context('when readPage reports failure', () => {
      const failureErr = new Error('failed!');

      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
            processResourceFn, success, failure
          }) => {
          failure(failureErr);
        });
        runGetAll();
      });

      it('propagates arguments to the client "finished" callback', () => {
        assert.calledOnce(finishedStub);
        assert.calledWithExactly(finishedStub, failureErr);
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
        runGetAll();
      });

      it('propagates the specific error', () => {
        assert.calledOnce(finishedStub);
        assert.calledWithExactly(finishedStub,
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
        runGetAll();
      });

      it('propagates arguments to the client "finished" callback', () => {
        assert.calledOnce(finishedStub);
        assert.calledWithExactly(finishedStub);
      });
    });

    context('when "whenFinished" is not called', () => {

      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
          processResourceFn, success, failure
        }) => {
          success();
        });
      });

      it('execution does not fail', () => {
        const client = serviceEventsClient.create(cfAdminToken,
          perf, statistics);
        const retriever = client.retriever({});

        retriever.start();
      });
    });
  });

  describe('verify events processing', () => {

    context('when forEachEvent is called', () => {
      const someCallback = () => {};

      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
            processResourceFn, success, failure
          }) => {
          processResourceFn(document, someCallback);
        });
        runGetAll();
      });

      it('propagates arguments to provided callback', () => {
        assert.calledOnce(processStub);
        assert.calledWithExactly(processStub, document, someCallback);
      });
    });

    context('when forEachEvent is NOT called', () => {
      const someCallbackStub = sandbox.stub();

      beforeEach(() => {
        readPageStub.callsFake((uri, cfToken, perf, statistics, {
          processResourceFn, success, failure
        }) => {
          processResourceFn(document, someCallbackStub);
        });

        const client = serviceEventsClient.create(cfAdminToken,
          perf, statistics);
        const retriever = client.retriever({});
        retriever.start();
      });

      it('should execute default callback', () => {
        assert.calledOnce(someCallbackStub);
      });
    });

  });
});
