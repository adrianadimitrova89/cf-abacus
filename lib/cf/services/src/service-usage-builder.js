'use strict';

const createCarryOver = require('abacus-carryover');
const moment = require('abacus-moment');
const states = require('./service-event-states');

const unsupportedEventError = new Error('Unsupported event type');

const translateEventToUsage = (state) => [
  {
    measure: 'current_instances',
    quantity: state === states.CREATED ? 1 : 0
  },
  {
    measure: 'previous_instances',
    quantity: state === states.CREATED ? 0 : 1
  }
];

const create = (statistics, registerError, eventChecker) => {

  return {
    buildServiceUsage: (event, cb) => {
      if (!event || !event.entity) {
        cb(new Error('Invalid state. '));
        return;
      }

      const serviceLabel = event.entity.service_label;
      const planName = event.entity.service_plan_name;

      if (!eventChecker.isSupported(event)) {
        cb(unsupportedEventError);
        return;
      }

      const eventTime = moment.utc(event.metadata.created_at).valueOf();
      const serviceGUID = `service:${event.entity.service_instance_guid}`;

      const usageDoc = {
        start: eventTime,
        end: eventTime,
        organization_id: event.entity.org_guid,
        space_id: event.entity.space_guid,
        consumer_id: serviceGUID,
        resource_id: serviceLabel,
        plan_id: planName,
        resource_instance_id: `${serviceGUID}:${planName}:${serviceLabel}`,
        measured_usage: translateEventToUsage(event.entity.state)
      };

      // Check for usage in the same second
      const carryOver = createCarryOver(statistics, registerError);
      carryOver.adjustTimestamp(usageDoc, event.metadata.guid, cb);
    }
  };
};

module.exports.create = create;
module.exports.unsupportedEventError = unsupportedEventError;

