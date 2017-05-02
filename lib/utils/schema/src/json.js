'use strict';

// JSON schema definition and validation utilities.

const _ = require('underscore');
const jsval = require('is-my-json-valid');
const lint = require('./lint.js');

const pairs = _.pairs;
const map = _.map;
const filter = _.filter;
const object = _.object;
const extend = _.extend;

const debug = require('abacus-debug')('abacus-schema');

// Convert a data type to a JSON schema
const schema = (t) => {
  return {
    string: (t) => ({
      type: 'string'
    }),
    number: (t) => ({
      type: 'number'
    }),
    time: (t) => ({
      type: 'integer',
      format: 'utc-millisec'
    }),
    functionString: (t) => ({
      type: 'string',
      format: t.name ? t.name + '-function' : 'function'
    }),
    arrayOf: (t) => ({
      type: 'array',
      minItems: t.minItems ? t.minItems : 0,
      items: schema(t.items),
      additionalItems: false
    }),
    enumType: (t) => ({
      title: t.name,
      description: t.description,
      enum: t.enum,
      default: t.default
    }),
    objectType: (t) => ({
      title: t.name,
      description: t.description,
      type: 'object',
      properties:
        object(map(pairs(t.properties), (p) => [p[0], schema(p[1])])),
      required:
        map(filter(pairs(t.properties), (p) => p[1].required), (p) => p[0]),
      additionalProperties: false
    }),
    unionType: (t) => ({
      title: t.name,
      description: t.description,
      anyOf: map(t.types, (type) => schema(type))
    }),
    anyType: (t) => ({
      title: t.name,
      description: t.description,
      anyOf: [
        {
          type: 'string'
        },
        {
          type: 'number'
        },
        {
          type: 'integer',
          format: 'utc-millisec'
        },
        {
          type: 'object'
        },
        {
          type: 'array'
        }
      ]
    })
  }[t.type](t);
};

const validateFunction = (code, doc, eslintConfigFile, validator, errors) => {
  if(process.env.VALIDATE_FUNCTIONS !== 'true')
    return true;

  // Check with ESLint
  const result = lint(code, eslintConfigFile);
  if(!result.ok) {
    result.errors.forEach((error) => errors.push(error));
    return false;
  }

  // Call the validator function if supplied
  if(validator)
    try {
      validator(code, doc);
    }
    catch(ex) {
      errors.push({
        message: ex.message,
        source: code
      });
      return false;
    }

  return true;
};

// Return a JSON Schema validator
const validator = (schema, eslintConfigFile, validators) => {
  return (doc) => {
    const errors = [];

    // Evaluate formats for schema validation
    const formats = extend({}, ...map(validators, (value, key) => {
      return {
        [key + '-function']: (code) =>
          validateFunction(code, doc, eslintConfigFile, value, errors)
      };
    }), {
      'function': (code) =>
        validateFunction(code, doc, eslintConfigFile, undefined, errors)
    });

    // Evaluate schema validation function
    const validate = jsval(schema, {
      verbose: true,
      greedy: true,
      formats: formats
    });

    // Perform the schema validation
    debug('Validating doducment %o with schema %o', doc, schema);
    const val = validate(doc);
    debug('Validation result for document %o is %o: %o', doc, val,
      validate.errors);
    if(!val)
      throw {
        statusCode: 400,
        message: validate.errors.concat(errors)
      };

    return doc;
  };
};

// Export our public functions
module.exports = schema;
module.exports.schema = schema;
module.exports.validator = validator;

