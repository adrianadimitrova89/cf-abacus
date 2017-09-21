'use strict';

const states = require('./service-event-states');

const supportedStates = [states.CREATED, states.DELETED];

const isStateSupported = (state) => {
  return supportedStates.includes(state);
};

const isServiceTracked = (serviceLabel, servicePlanName, services) => {
  if (services === undefined)
    return false;

  const serviceConfig = services[serviceLabel];
  if (serviceConfig === undefined)
    return false;

  return serviceConfig.plans.includes(servicePlanName);
};

const create = (services) => {
  return {
    isSupported: (event) => {
      return isStateSupported(event.entity.state)
        && isServiceTracked(
          event.entity.service_label,
          event.entity.service_plan_name,
          services);
    }
  };
};

module.exports.create = create;
