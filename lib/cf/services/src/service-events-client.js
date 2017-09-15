'use strict';

const paging = require('abacus-paging');
const qs = require('querystring');

const resultsPerPage = 50;

const guidNotFoundError =
  new Error('Service event with specified GUID not found.');

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
  const retriever = ({ serviceGuids, afterGuid }) => {

    let processEvent = (doc, cb) => cb();
    let processingFinished = () => {};

    return {
      forEachEvent: (processFn) => {
        processEvent = processFn;
      },
      whenFinished: (finishedFn) => {
        processingFinished = finishedFn;
      },
      start: () => {
        const url = eventsURL({ serviceGuids, afterGuid });
        paging.readPage(url, cfAdminToken, perf, statistics, {
          processResourceFn: (doc, cb) => {
            processEvent(doc, cb);
          },
          success: () => {
            processingFinished();
          },
          failure: (err, response) => {
            const isGuidNotFoundResponse =
              response && response.statusCode === 400 &&
              response.body && response.body.code === 10005;
            if (isGuidNotFoundResponse)
              processingFinished(guidNotFoundError);
            else
              processingFinished(err);
          }
        });
      }
    };
  };

  return {
    retriever
  };
};

module.exports.create = create;
module.exports.guidNotFoundError = guidNotFoundError;
