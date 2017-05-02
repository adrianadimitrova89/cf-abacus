'use strict';

// The data schemas we use to validate metering and rating plans and usage
// data.

const _ = require('underscore');
const path = require('path');
const BigNumber = require('bignumber.js');
const schema = require('abacus-schema');
const xeval = require('abacus-eval');

const map = _.map;
const extend = _.extend;

// Import our data types
const meteringPlan = require('./metering-plan.js');
const pricingPlan = require('./pricing-plan.js');
const resourceUsage = require('./resource-usage.js');
const organizationReport = require('./organization-report.js');
const ratingPlan = require('./rating-plan.js');
const resourceInstanceReport = require('./resource-instance-report.js');

const debug = require('abacus-debug')('abacus-usage-schemas');

const eslintConfigFile = path.resolve(__dirname, '../config/.eslintrc');

// Function call results cache
const results = {};

/* eslint complexity: [1, 7] */
const validator = (name) => (code, doc) => {
  // Compose a cache key prefix based on plan id and metric name
  const metric = doc.metrics.find((metric) => metric[name] === code);
  const prefix = doc.plan_id + '/' + (metric ? metric.name : '');

  // Evaluate the function
  debug('Evaluating %s function of plan %s / metric %s with code %s',
    name, doc.plan_id, metric.name, code);
  const f = xeval(code, { BigNumber: BigNumber });

  // Compose appropriate function arguments
  let args = [];
  if (name === 'meter') { // m
    const m = extend({}, ...map(doc.measures, (measure) => {
      return !measure.name ? {} : {
        [measure.name]: 1
      };
    }));
    args = [m];
  }
  else if (name === 'accumulate') // a, qty, start, end, from, to, twCell
    args = [undefined, results[prefix + '/meter'] || 1, 0, 100, 50, 150,
      undefined];
  else if (name === 'aggregate') // a, prev, curr, aggTwCell, accTwCell
    args = [undefined, results[prefix + '/accumulate'] || 1,
      results[prefix + '/accumulate'] || 1, undefined, undefined];
  else if (name === 'summarize') // t, qty, from, to
    args = [100, results[prefix + '/aggregate'] || 1, 50, 150];

  // Call the function and store the result in the results cache
  debug('Calling %s function with arguments %o', name, args);
  const result = f(...args);
  debug('%s function returned result %o', name, result);
  results[prefix + '/' + name] = result;
};

const validators = {
  'meter': validator('meter'),
  'accumulate': validator('accumulate'),
  'aggregate': validator('aggregate'),
  'summarize': validator('summarize')
};

// Compile a type into a JSON schema, GraphQL schema and validate function
const compile = (type) => {
  const json = schema.json(type);
  const graph = schema.graph(type);
  return {
    type: () => type,
    json: () => json,
    graph: () => graph,
    validate: schema.validator(json, eslintConfigFile, validators)
  };
};

// Export the compiled types
module.exports.meteringPlan = compile(meteringPlan());
module.exports.pricingPlan = compile(pricingPlan());
module.exports.resourceUsage = compile(resourceUsage());
module.exports.organizationReport = compile(organizationReport());
module.exports.ratingPlan = compile(ratingPlan());
module.exports.resourceInstanceReport = compile(resourceInstanceReport());

