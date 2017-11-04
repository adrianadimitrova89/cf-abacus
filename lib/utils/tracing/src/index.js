'use strict';

const _ = require('underscore');
const opentracing = require('opentracing');
const rootContext = require('abacus-context');
const tracer = require('./tracer');
const map = _.map;
const without = _.without;

const createFanInSpan = (contexts, name) => {
  const parentSpans = without(
    map(contexts, (context) => context && context.get('span')), undefined);

  const references = map(parentSpans, 
    (pSpan) => opentracing.childOf(pSpan.context()));
  console.log('Creating fan-in with %d references', references.length);
  const newSpan = tracer.startSpan(name, {
    references: references
  });
  const newContext = rootContext.extend({
    span: newSpan
  });
  return {
    span: newSpan,
    context: newContext
  };
};

const createSpan = (context, name) => {
  const parentSpan = context.get('span');
  const newSpan = parentSpan 
    ? tracer.startSpan(name, {
      childOf: parentSpan
    })
    : tracer.startSpan(name);
  const newContext = context.extend({
    span: newSpan
  });
  return {
    span: newSpan,
    context: newContext
  };
};

const tagServerRequest = (span, req) => {
  span.setTag(opentracing.Tags.SPAN_KIND, 'server');
  span.setTag('request-method', req.method);
  span.setTag('request-url', req.url);
};

const tagServerResponse = (span, resp) => {
  span.setTag('response-status-code', resp.statusCode);
};

const middleware = (req, resp, next) => {
  const context = req.context || rootContext;

  const parentSpan = (() => {
    if (context.get('span'))
      return context.get('span');

    const headers = req.headers || {};
    return tracer.extract(opentracing.FORMAT_HTTP_HEADERS, headers);
  })();
  const span = tracer.startSpan(`[INBOUND]: ${req.method} ${req.url}`, {
    childOf: parentSpan
  });

  tagServerRequest(span, req);
  req.context = context.extend({
    span
  });

  resp.on('finish', () => {
    tagServerResponse(span, resp);
    span.finish();
  });

  next();
};

const request = (original) => {
  return (target, cb) => {
    const context = target.options.context || rootContext;
    const name = `[OUTBOUND]: ${target.options.method} ${target.uri}`;
    const { span } = Array.isArray(context)
      ? createFanInSpan(context, name)
      : createSpan(context, name);

    span.setTag(opentracing.Tags.SPAN_KIND, 'client');
    span.setTag('request-method', target.options.method);
    span.setTag('request-url', target.uri);

    target.options.headers = target.options.headers || {};
    const headers = target.options.headers;
    tracer.inject(span.context(), opentracing.FORMAT_HTTP_HEADERS, headers);

    return original(target, (err, ...args) => {
      if (err) {
        span.setTag(opentracing.Tags.ERROR, true);
        span.setTag('message', err.message);
        span.setTag('stack', err.stack);
      }
      span.finish();
      return cb(err, ...args);
    });
  };
};


const runSpan = function *(context, name, delegate) {
  const { context: subContext, span: subSpan } = createSpan(context, name);
  try {
    return yield delegate(subContext, subSpan);
  }
  catch (err) {
    subSpan.setTag(opentracing.Tags.ERROR, true);
    subSpan.setTag('message', err.message);
    subSpan.setTag('stack', err.stack);
    throw err;
  }
  finally {
    subSpan.finish();
  }
};

module.exports.tracer = tracer;
module.exports.middleware = middleware;
module.exports.request = request;
module.exports.createSpan = createSpan;
module.exports.createFanInSpan = createFanInSpan;
module.exports.runSpan = runSpan;
