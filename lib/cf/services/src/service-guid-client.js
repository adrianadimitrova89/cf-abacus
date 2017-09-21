'use strict';

const paging = require('abacus-paging');
const _ = require('underscore');
const filter = _.filter;

const debug = require('abacus-debug')('abacus-cf-services-guid-client');
const edebug = require('abacus-debug')('e-abacus-cf-services-guid-client');

const serviceLabelsWithMissingGuid = (services) => {
  return filter(Object.keys(services), (key) => !services[key].guid);
};

const injectGuids = (services, cfAdminToken, perf, statistics, cb) => {
  const servicesLabels = serviceLabelsWithMissingGuid(services);
  if (servicesLabels.length === 0) {
    cb();
    return;
  }

  const filter = encodeURIComponent(`label IN ${servicesLabels.join(',')}`);
  const url = `/v2/services?q=${filter}`;

  paging.readPage(url, cfAdminToken, perf, statistics, {
    processResourceFn: (service, done) => {
      debug('Got service resources %j', service);
      services[service.entity.label].guid = service.metadata.guid;
      done();
    },
    failure: (error, response) => {
      edebug('Could not read service guids from CC due to %j, %j',
       error, response);
      if (error)
        cb(error);
      else
        cb(new Error(
        `Could not read service guids from CC due to ${error}, ${response}`));
    },
    success: () => {
      cb();
    }
  });
};

const create = (cfAdminToken, perf, statistics) => {
  return {
    injectGuids: (services, cb) => {
      injectGuids(services, cfAdminToken, perf, statistics, cb);
    }
  };
};

module.exports.create = create;