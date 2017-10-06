'use strict';

const cluster = require('abacus-cluster');
const EventEmitter = require('events');

const stubModule = require('./stubber');
const executeModuleStub = stubModule('../executor');

const runApplication = require('../application');

describe('application', () => {
  const sandbox = sinon.sandbox.create();

  const globalConfig = {
    shared: 'properties'
  };
  const job = 'fake-job';
  const web = 'fake-web';

  let loadStub;
  let createJobStub;
  let createWebStub;
  let executeStub;
  let clusterSingletonStub;
  let clusterIsWorkerStub;

  beforeEach(() => {
    loadStub = sandbox.stub();
    createJobStub = sandbox.stub();
    createWebStub = sandbox.stub();
    executeStub = sandbox.stub();
    executeModuleStub.stubMainFunc(executeStub);
    executeStub.returns(new EventEmitter());
    clusterSingletonStub = sandbox.stub(cluster, 'singleton');
    clusterIsWorkerStub = sandbox.stub(cluster, 'isWorker');
  });

  afterEach(() => {
    sandbox.restore();
  });

  const runApplicationUnderTest = (cb) => {
    runApplication({
      load: loadStub,
      createJob: createJobStub,
      createWeb: createWebStub
    }, cb);
  };

  context('when all components work', () => {
    const assertJobCreated = () => {
      assert.calledOnce(createJobStub);
      assert.calledWith(createJobStub, globalConfig);
    };

    const assertWebCreated = () => {
      assert.calledOnce(createWebStub);
      assert.calledWith(createWebStub, globalConfig);
    };

    beforeEach(() => {
      loadStub.yields(undefined, globalConfig);
      createJobStub.yields(undefined, job);
      createWebStub.yields(undefined, web);
    });

    it('executes in singleton mode', () => {
      runApplicationUnderTest();
      assert.calledOnce(clusterSingletonStub);
    });

    context('when master', () => {
      beforeEach(() => {
        clusterIsWorkerStub.returns(false);
      });

      it('run executes only web', (done) => {
        runApplicationUnderTest((err) => {
          assert.calledOnce(loadStub);

          assert.notCalled(createJobStub);
          assertWebCreated();

          assert.calledOnce(executeStub);
          assert.calledWithExactly(executeStub, web);

          done(err);
        });
      });
    });

    context('when worker', () => {
      beforeEach(() => {
        clusterIsWorkerStub.returns(true);
      });

      it('run executes job and web', (done) => {
        runApplicationUnderTest((err) => {
          assert.calledOnce(loadStub);

          assertJobCreated();
          assertWebCreated();

          assert.calledTwice(executeStub);
          assert.calledWithExactly(executeStub, job);
          assert.calledWithExactly(executeStub, web);

          done(err);
        });
      });

      context('when job creation fails', () => {
        beforeEach(() => {
          createJobStub.yields(new Error('Failed to create job'));
        });

        it('does not execute anything', (done) => {
          runApplicationUnderTest((err) => {
            expect(err).to.not.equal(undefined);
            assert.notCalled(executeStub);
            done();
          });
        });
      });

      context('when web creation fails', () => {
        beforeEach(() => {
          createWebStub.yields(new Error('Failed to create web'));
        });

        it('does not execute anything', (done) => {
          runApplicationUnderTest((err) => {
            expect(err).to.not.equal(undefined);
            assert.notCalled(executeStub);
            done();
          });
        });
      });
    });
  });
});
