'use strict';

const cluster = require('abacus-cluster');
const yieldable = require('abacus-yieldable');
const functioncb = yieldable.functioncb;
const execute = require('./executor');

const debug = require('abacus-debug')('abacus-bridge-application');

const run = (opts, cb) => {
  cluster.singleton();

  const load = yieldable(opts.load);
  const createJob = yieldable(opts.createJob);
  const createWeb = yieldable(opts.createWeb);

  const startJob = (executable) => {
    execute(executable)
      .on('start-success', () => {
        debug('Started background job.');
      })
      .on('start-failure', (err) => {
        edebug('Failed to start background job: %s', err);
        throw err;
      })
      .on('stop-success', () => {
        debug('Stopped background job.');
      })
      .on('stop-failure', (err) => {
        edebug('Failed to stop background job: %s', err);
        throw err;
      });
  };

  const startWeb = (executable) => {
    execute(executable)
    .on('start-success', () => {
      debug('Started server.');
    })
    .on('start-failure', (err) => {
      edebug('Failed to start server: %s', err);
      throw err;
    })
    .on('stop-success', () => {
      debug('Stopped server.');
    })
    .on('stop-failure', (err) => {
      edebug('Failed to stop server: %s', err);
      throw err;
    });
  };

  functioncb(function *() {
    const config = yield load();

    const job = cluster.isWorker() ?
      yield createJob(config) :
      undefined;
    const web = yield createWeb(config);

    if (cluster.isWorker())
      startJob(job);
    startWeb(web);
  })(cb);
};

module.exports = run;
