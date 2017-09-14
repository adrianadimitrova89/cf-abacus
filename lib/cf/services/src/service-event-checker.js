'use strict';

const isStateSupported = (state) => {
  const supportedStates = ['CREATED', 'DELETED'];
  return supportedStates.includes(state);
};

const shouldReportServiceEvents = (serviceLabel, services) => {
  return services != undefined && services[serviceLabel] != undefined;
};

const shouldReportPlanEvents = (serviceLabel, servicePlanName, services) => {
  return services != undefined
    && services[serviceLabel].plans.includes(servicePlanName);
};

const create = (services) => {

  return {
    isSupported: (event) => {
      return isStateSupported(event.entity.state)
        && shouldReportServiceEvents(event.entity.service_label, services)
        && shouldReportPlanEvents(
          event.entity.service_label,
          event.entity.service_plan_name,
          services);
    }
  };
};

module.exports.create = create;
