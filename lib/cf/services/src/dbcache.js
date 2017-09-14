'use strict';

const batch = require('abacus-batch');
const breaker = require('abacus-breaker');
const dbclient = require('abacus-dbclient');
const moment = require('abacus-moment');
const partition = require('abacus-partition');
const perf = require('abacus-perf');
const retry = require('abacus-retry');
const throttle = require('abacus-throttle');
const urienv = require('abacus-urienv');
const _ = require('underscore');
const extend = _.extend;

const debug = require('abacus-debug')('abacus-cf-services-cache');
const edebug = require('abacus-debug')('e-abacus-cf-services-cache');

const create = (config, statistics) => {
  const documentID = config.documentID;

  const uris = urienv({
    [config.alias]: 5984
  });

  const db = throttle(retry(breaker(batch(
    dbclient(partition.singleton, dbclient.dburi(uris[config.alias],
      'abacus-cf-bridge'))
  ))));

  let revision;

  const read = (cb) => {
    debug('Reading value with id "%s"...', documentID);
    const readStart = moment.now();
    db.get(documentID, (err, doc) => {
      if (err) {
        edebug('Failed to read value with id "%s"!', documentID);
        statistics.failedReads++;
        cb(err);
        return;
      }

      if (doc)
        revision = doc._rev;

      debug('Successfully read value with id "%s": %o', documentID, doc);
      perf.report('cache.read', readStart);
      statistics.successfulReads++;
      cb(undefined, doc);
    });
  };

  const write = (value, cb) => {
    const newDocument = extend({}, value, { _id: documentID, _rev: revision });
    debug('Writing value with id "%s": %o...', documentID, newDocument);
    const writeStart = moment.now();
    db.put(newDocument, (err, doc) => {
      if (err) {
        edebug('Failed to write value with id "%s": %o. Error: %o',
          documentID, newDocument, err);
        statistics.failedWrites++;
        cb(err);
        return;
      }

      revision = doc.rev;
      debug('Successfully wrote value with id "%s": %o', documentID, doc);
      perf.report('cache.write', writeStart);
      statistics.successfulWrites++;
      cb();
    });
  };

  return {
    read,
    write
  };
};

const createStatistics = () => {
  return {
    failedReads: 0,
    successfulReads: 0,
    failedWrites: 0,
    successfulWrites: 0
  };
};

module.exports.create = create;
module.exports.createStatistics = createStatistics;
