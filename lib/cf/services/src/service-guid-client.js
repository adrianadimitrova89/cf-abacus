'use strict';

const paging = require('abacus-paging');
const _ = require('underscore');
const filter = _.filter;

const debug = require('abacus-debug')('abacus-cf-services-guid-client');
const edebug = require('abacus-debug')('e-abacus-cf-services-guid-client');

const serviceLabelsWithMissingGuid = (services) => {
  return filter(Object.keys(services), (key) => !services[key].guid);
};

const injectGuids = (cfAdminToken, perf, statistics, services, cb) => {
  const servicesLabels = serviceLabelsWithMissingGuid(services);
  if (servicesLabels.length === 0) {
    cb();
    return;
  }

  let url = '/v2/services?q=label IN ' + servicesLabels.join(',');

  paging.readPage(url, cfAdminToken, perf, statistics, {
    processResourceFn: (service, done) => {
      debug('Got service resources %j', service);
      services[service.entity.label].guid = service.metadata.guid;
      done();
    },
    failure: (error, response) => {
      edebug('Could not read service guids from CC due to' +
      '%j, %j', error, response);
      cb(error);
    },
    success: () => {
      cb();
    }
  });
};

module.exports.create = (cfAdminToken, perf, statistics) => {

  return {
    injectGuids: (services, cb) => {
      injectGuids(cfAdminToken, perf, statistics, services, cb);
    }
  };
};
