'use strict';

const moment = require('abacus-moment');
const paging = require('abacus-paging');

const qs = require('querystring');

const debug = require('abacus-debug')('abacus-cf-services-events-client');
const edebug = require('abacus-debug')('e-abacus-cf-services-events-client');

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

const isOrgEnabled = (resource, orgs) =>
  orgs ? orgs.includes(resource.entity.org_guid) : true;

/**
 * "The list of usage events returned from the API is not guaranteed to be
 * complete. Events may still be processing at the time of the query, so
 * events that occurred before the final event may still appear
 * [...]
 * it is recommended that operators select their ‘after_guid’ from an event
 * far enough back in time to ensure that all events have been processed"
 *
 * https://www.cloudfoundry.org/how-to-bill-on-cloud-foundry/
 */
const isOldEnough = (resource, minAge) => {
  const now = moment.now();
  const resourceDate = moment.utc(resource.metadata.created_at).valueOf();
  const age = now - resourceDate;
  const oldEnough = age > minAge;
  debug('Resource %s has age %d. Minimum resource age is %d. Old enough: %s',
    resource.metadata.guid, age, minAge, oldEnough);

  return oldEnough;
};

const processable = (doc, options) => {
  return isOrgEnabled(doc, options.orgsToReport)
    && isOldEnough(doc, options.minAge);
};

const create = (options) => {
  const retriever = ({ serviceGuids, afterGuid }) => {

    let processEvent;
    let successed = () => {};
    let failed = () => {};

    return {
      forEachEvent: (processFn) => {
        processEvent = processFn;
      },
      whenSuccessed: (successedFn) => {
        successed = successedFn;
      },
      whenFailed: (failedFn) => {
        failed = failedFn;
      },
      start: () => {
        const url = eventsURL({ serviceGuids, afterGuid });
        paging.readPage(url, options.cfAdminToken, options.perf,
          options.statistics, {
            processResourceFn: (doc, cb) => {
              if (!processEvent) {
                cb();
                return;
              }

              if(processable(doc, options))
                processEvent(doc, cb);
            },
            success: () => {
              successed();
            },
            failure: (err, response) => {
              const isGuidNotFoundResponse =
                response && response.statusCode === 400 &&
                response.body && response.body.code === 10005;
              if (isGuidNotFoundResponse)
                failed(guidNotFoundError);
              else {
                edebug(
                  'Could not process events due to error: %j, response: %j',
                    err, response);
                failed(new Error(
                  `Could not process events due to "${err}":"${response}"`));
              }
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
