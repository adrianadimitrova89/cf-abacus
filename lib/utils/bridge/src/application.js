'use strict';

const cluster = require('abacus-cluster');
const yieldable = require('abacus-yieldable');
const functioncb = yieldable.functioncb;

const execute = require('./executor');

const run = (opts, cb) => {
  cluster.singleton();

  const load = yieldable(opts.load);
  const createJob = yieldable(opts.createJob);
  const createWeb = yieldable(opts.createWeb);

  functioncb(function *() {
    const config = yield load();

    const job = cluster.isWorker() ?
      yield createJob(config) :
      undefined;
    const web = yield createWeb(config);

    if (cluster.isWorker())
      execute(job);
    execute(web);
  })(cb);
};

module.exports = run;
