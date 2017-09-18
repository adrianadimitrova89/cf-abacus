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
  let finishedStub;

  beforeEach(() => {
    processStub = sandbox.stub();
    finishedStub = sandbox.stub();
    readPageStub = sandbox.stub(paging, 'readPage');
  });

  afterEach(() => {
    sandbox.restore();
  });

  const runGetAll = ({ serviceGuids, afterGuid, orgsToReport } = {}) => {
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

  describe('verify events processing finished', () => {
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
            runGetAll();
          });

          it('propagates arguments to provided callback', () => {
            assert.calledOnce(processStub);
            assert.calledWithExactly(processStub, document, someCallback);
          });
        });

        context('when event is not old enough', () => {

          beforeEach(() => {
            moment.now.returns(documentCreationTime + minAge - 1);
            runGetAll();
          });

          it('callback is never called', () => {
            assert.notCalled(processStub);
          });

        });
      });

      context('when event\'s org is not in the enabled orgs', () => {

        beforeEach(() => {
          runGetAll({ orgsToReport: ['nonreportable-org-guid'] });
        });

        it('callback is never called', () => {
          assert.notCalled(processStub);
        });

      });

      context('when event\'s org is in the enabled orgs', () => {

        beforeEach(() => {
          runGetAll({ orgsToReport: [testOrg] });
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
