'use strict';

const paging = require('abacus-paging');
const qs = require('querystring');

const resultsPerPage = 50;

const missingErr = new Error('Service event with specified GUID not found.');

const eventsURL = ({ serviceGuids, afterGuid }) => {
  const path = '/v2/service_usage_events';
  const queries = {
    'order-direction': 'asc',
    'results-per-page': resultsPerPage,
    'q' : ['service_instance_type:managed_service_instance']
  };
  if (serviceGuids && serviceGuids.length > 0) {
    const filter = `service_guid IN ${serviceGuids.join(',')}`;
    queries.q.push(filter);
  }
  if (afterGuid)
    queries.after_guid = afterGuid;
  return `${path}?${qs.stringify(queries)}`;
};

const create = (cfAdminToken, perf, statistics) => {
  const getAll = ({ serviceGuids, afterGuid }, { process, finished }) => {
    const url = eventsURL({ serviceGuids, afterGuid });
    paging.readPage(url, cfAdminToken, perf, statistics, {
      processResourceFn: (doc, cb) => {
        process(doc, cb);
      },
      success: () => {
        finished();
      },
      failure: (err, response) => {
        const isGuidNotFoundResponse =
          response && response.statusCode === 400 &&
          response.body && response.body.code === 10005;
        if (isGuidNotFoundResponse)
          finished(missingErr);
        else
          finished(err);
      }
    });
  };

  return {
    getAll
  };
};

module.exports.create = create;
module.exports.missingErr = missingErr;
