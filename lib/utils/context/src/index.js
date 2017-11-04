'use strict';

const _ = require('underscore');

const createContext = (initProperties = {}) => {
  let properties = initProperties;

  const get = (key) => {
    return properties[key];
  };

  const extend = (extendedProperties) => {
    return createContext(
      _.extend({}, properties, extendedProperties)
    );
  };

  return {
    get,
    extend
  };
};

const rootContext = createContext();

module.exports = rootContext;
