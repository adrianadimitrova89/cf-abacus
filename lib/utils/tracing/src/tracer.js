'use strict';

const initTracer = require('jaeger-client').initTracer;

const createTracer = (opts) => {
  const config = {
    'serviceName': opts.serviceName,
    'sampler': {
      'type': 'const',
      'param': 1,
      'refreshIntervalMs': 250
    },
    'reporter': {
      'logSpans': false,
      'flushIntervalMs': 250
    }
  };
  const options = {
    'tags': {
      [`${opts.serviceName}.version`]: opts.serviceVersion
    }
  };
  return initTracer(config, options);
};

const tracer = createTracer({
  serviceName: process.env.TRACING_SERVICE_NAME || 'abacus',
  serviceVersion: process.env.TRACING_SERVICE_VERSION || '1.0.0'
});

process.on('SIGTERM', () => {
  console.log('Tracer: Shutting down due to SIGTERM');
  tracer.close();
});

process.on('exit', () => {
  console.log('Tracer: Shutting down due to EXIT');
  tracer.close();
});

module.exports = tracer;
