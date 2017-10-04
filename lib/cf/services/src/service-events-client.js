'use strict';

const util = require('util');
const qs = require('querystring');
const moment = require('abacus-moment');
const paging = require('abacus-paging');
const perf = require('abacus-perf');

const debug = require('abacus-debug')('abacus-cf-services-events-client');
const edebug = require('abacus-debug')('e-abacus-cf-services-events-client');

const resultsPerPage = 50;

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

const isGuidNotFoundResponse = (response) => {
  return response && response.statusCode === 400 &&
    response.body && response.body.code === 10005;
};

const createGuidNotFoundError = (guid) => {
  const msg = util.format('Event with GUID "%s" not found.', guid);
  const err = new Error(msg);
  err.guidNotFound = true;
  return err;
};

const create = (options) => {
  const retriever = ({ serviceGuids, afterGuid }) => {
    let processEvent;
    let succeeded = () => {};
    let failed = () => {};

    const onReadPageDocument = (doc, cb) => {
      if (!processEvent) {
        cb();
        return;
      }

      if(isOldEnough(doc, options.minAge))
        processEvent(doc, cb);
      else
        cb();
    };

    const onReadPageSuccess = () => succeeded();

    const onReadPageFailure = (err, response) => {
      if (isGuidNotFoundResponse(response))
        failed(createGuidNotFoundError(afterGuid));
      else {
        edebug('Could not process events due to error: %j, response: %j',
            err, response);
        const msg = util.format(
          'Could not read events due to error "%s" and reponse "%s".',
          err, response);
        failed(new Error(msg));
      }
    };

    return {
      forEachEvent: (processFn) => {
        processEvent = processFn;
      },
      whenSucceeded: (succeededFn) => {
        succeeded = succeededFn;
      },
      whenFailed: (failedFn) => {
        failed = failedFn;
      },
      start: () => {
        const url = eventsURL({ serviceGuids, afterGuid });
        paging.readPage(url, options.cfAdminToken, perf,
          options.statistics, {
            processResourceFn: onReadPageDocument,
            success: onReadPageSuccess,
            failure: onReadPageFailure
          });
      }
    };
  };

  return {
    retriever
  };
};

module.exports = create;
