'use strict';

const _ = require('underscore');
const extend = _.extend;
const stubbedmodule = require('./stubber');

const abacusDBClientModule = stubbedmodule('abacus-dbclient');
const abacusBatchModule = stubbedmodule('abacus-batch');
const abacusRetryModule = stubbedmodule('abacus-retry');
const abacusBreakerModule = stubbedmodule('abacus-breaker');
const abacusThrottleModule = stubbedmodule('abacus-throttle');
const abacusURIEnvModule = stubbedmodule('abacus-urienv');
const dbcache = require('../dbcache');

describe('service-bridge/dbcache', () => {
  const sandbox = sinon.sandbox.create();
  const dbConfig = {
    alias: 'db',
    documentID: 'docId'
  };
  let cache;
  let statistics;
  let dbGetStub;
  let dbPutStub;
  let dbURIStub;

  beforeEach(() => {
    abacusBatchModule.stubMainFunc((fn) => fn);
    abacusRetryModule.stubMainFunc((fn) => fn);
    abacusBreakerModule.stubMainFunc((fn) => fn);
    abacusThrottleModule.stubMainFunc((fn) => fn);

    const uriEnvStub = sandbox.stub();
    uriEnvStub.withArgs({
      db: 5984
    }).callsFake(() => ({
      db: 'http://localhost:1234'
    }));
    abacusURIEnvModule.stubMainFunc(uriEnvStub);

    dbGetStub = sandbox.stub();
    dbPutStub = sandbox.stub();
    dbURIStub = sandbox.stub();
    abacusDBClientModule.stubMainFunc(() => ({
      get: dbGetStub,
      put: dbPutStub
    })).stubProperties({
      dburi: dbURIStub
    });

    statistics = dbcache.createStatistics();
    cache = dbcache.create(dbConfig, statistics);
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('creates the correct uri', () => {
    assert.calledWithExactly(dbURIStub,
      'http://localhost:1234', 'abacus-cf-bridge');
  });

  context('when unexisting data is read', () => {
    let actualError;
    let actualValue;

    beforeEach((done) => {
      dbGetStub.callsFake((id, cb) => {
        cb();
      });

      cache.read((err, value) => {
        actualError = err;
        actualValue = value;
        done();
      });
    });

    it('expect no error is returned', () => {
      expect(actualError).to.equal(undefined);
    });

    it('expect "undefined" value is returned', () => {
      expect(actualValue).to.equal(undefined);
    });
  });

  context('when existing data is read', () => {
    const cacheValue = {
      _id: dbConfig.documentID,
      _rev: 3,
      guid: 'some-guid'
    };
    let readErr;
    let readValue;

    beforeEach((done) => {
      dbGetStub.callsFake((id, cb) => {
        expect(id).to.equal(dbConfig.documentID);
        cb(undefined, cacheValue);
      });

      cache.read((err, value) => {
        readErr = err;
        readValue = value;
        done();
      });
    });

    it('returns stored value', () => {
      expect(readErr).to.equal(undefined);
      expect(readValue).to.equal(cacheValue);
      expect(statistics.failedReads).to.equal(0);
      expect(statistics.successfulReads).to.equal(1);
    });

    context('when value is written', () => {
      const newCacheValue = {
        guid: 'some-new-guid'
      };
      let writtenDoc;
      let writeErr;

      beforeEach((done) => {
        dbPutStub.onFirstCall().callsFake((doc, cb) => {
          writtenDoc = doc;
          cb(undefined, extend({}, doc, { _rev: 4 }));
        });

        cache.write(newCacheValue, (err) => {
          writeErr = err;
          done();
        });
      });

      it('stores correct document (and revision) in db', () => {
        expect(writtenDoc).to.deep.equal({
          _id: dbConfig.documentID,
          _rev: 3,
          guid: newCacheValue.guid
        });
        expect(writeErr).to.equal(undefined);
        expect(statistics.failedWrites).to.equal(0);
        expect(statistics.successfulWrites).to.equal(1);
      });

      context('when yet another value is written', () => {
        const newestCacheValue = {
          guid: 'some-last-guid'
        };
        let newestWrittenDoc;
        let newestWriteErr;

        beforeEach((done) => {
          dbPutStub.onSecondCall().callsFake((doc, cb) => {
            newestWrittenDoc = doc;
            cb(undefined, extend({}, doc, { _rev: 5 }));
          });

          cache.write(newestCacheValue, (err) => {
            newestWriteErr = err;
            done();
          });
        });

        it('stores correct (and revision) document in db', () => {
          expect(newestWrittenDoc).to.deep.equal({
            _id: dbConfig.documentID,
            _rev: 4,
            guid: newestWrittenDoc.guid
          });

          expect(newestWriteErr).to.equal(undefined);
          expect(statistics.failedWrites).to.equal(0);
          expect(statistics.successfulWrites).to.equal(2);
        });
      });
    });
  });

  context('when db get fails', () => {
    const dbErr = new Error('failed!');

    beforeEach(() => {
      dbGetStub.callsFake((id, cb) => {
        cb(dbErr, undefined);
      });
    });

    it('read fails and propagates error', (done) => {
      cache.read((err, value) => {
        expect(err).to.equal(dbErr);
        expect(value).to.equal(undefined);
        expect(statistics.failedReads).to.equal(1);
        expect(statistics.successfulReads).to.equal(0);
        done();
      });
    });
  });

  context('when db put fails', () => {
    const cacheValue = {
      _id: dbConfig.documentID,
      _rev: 1,
      guid: 'some-guid'
    };
    const dbErr = new Error('failed!');

    beforeEach(() => {
      dbPutStub.callsFake((doc, cb) => {
        cb(dbErr, undefined);
      });
    });

    it('write fails and propagates error', (done) => {
      cache.write(cacheValue, (err) => {
        expect(err).to.equal(dbErr);
        expect(statistics.failedWrites).to.equal(1);
        expect(statistics.successfulWrites).to.equal(0);
        done();
      });
    });
  });
});
