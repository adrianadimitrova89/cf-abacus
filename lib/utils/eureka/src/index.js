'use strict';

// A simple Netflix Eureka client.

const request = require('abacus-request');
const urienv = require('abacus-urienv');

const debug = require('abacus-debug')('abacus-eureka');
const edebug = require('abacus-debug')('e-abacus-eureka');

const uris = urienv({
  eureka: 9990
});

const server = (host) => {
  const s = host ? host : process.env.EUREKA ? uris.eureka : undefined;
  return s ? s + '/eureka/v2' : undefined;
};

const secured = process.env.SECURED === 'true';

const authentication = () => secured ?
  `${process.env.EUREKA_USER}:${process.env.EUREKA_PASSWORD}` : undefined;

const register = (server, app, appindex, iindex, uri, port, cb) => {
  debug('Registering app %s %s instance %s uri %s port %s',
    app, appindex, iindex, uri, port);

  // Try to register every 5 seconds until it succeeds
  const retry = setInterval(() => {
    request.post(server + '/apps/:app', {
      app: app.toUpperCase(),
      auth: authentication(),
      body: {
        instance: {
          dataCenterInfo: {
            '@class': 'com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo',
            name: 'MyOwn'
          },
          app: app.toUpperCase(),
          asgName: app.toUpperCase(),
          hostName: uri,
          ipAddr: uri,
          vipAddress: uri,
          port: {
            $: port,
            '@enabled': true
          },
          metadata: {
            port: port
          },
          status: 'UP'
        }
      }
    }, (err, val) => {
      if(err || val && val.statusCode !== 204) {
        edebug(`Couldn't register app ${app} ${appindex} instance ${iindex} ` +
          `uri ${uri} port ${port}, %o %o`, err, val);
        return;
      }

      debug('Registered app %s %s instance %s uri %s port %s',
        app, appindex, iindex, uri, port);
      clearInterval(retry);
      cb(err, val);
    });
  }, process.env.EUREKA_REGISTER_INTERVAL || 5000);

  // Make sure the interval doesn't prevent the process to end
  retry.unref();
};

const deregister = (server, app, appindex, iindex, cb) => {
  debug('Deregistering app %s %s instance', app, appindex, iindex);
  request.delete(server + '/apps/:app/:instance', {
    app: app.toUpperCase(),
    instance: [[app, appindex].join('-'), iindex].join('.'),
    auth: authentication()
  }, (err, val) => {
    if(err || val && val.statusCode !== 200)
      edebug(`Couldn't deregister app ${app} ${appindex} instance ${iindex} %o`,
        err);
    else
      debug(`Deregistered app ${app} ${appindex} instance ${iindex}`);
    cb(err, val);
  });
};

const instance = (server, app, appindex, iindex, cb) => {
  debug(`Looking up app ${app} ${appindex} instance ${iindex}`);
  request.get(server + '/apps/:app/:instance', {
    app: app.toUpperCase(),
    instance: [[app, appindex].join('-'), iindex].join('.'),
    auth: authentication()
  }, (err, val) => {
    if(err) {
      edebug(`Error looking up app ${app} ${appindex} instance ${iindex}, %o`,
        err);
      return cb(err);
    }
    if(val.statusCode !== 200 || !val.body) {
      edebug(`App ${app} ${appindex} instance ${iindex} not found`);
      return cb();
    }

    debug('Found app %s %s instance %s info %o',
      app, appindex, iindex, val.body);
    const idoc = val.body.instance;
    return cb(undefined, {
      app: idoc.app,
      instance: idoc.hostName,
      address: idoc.ipAddr,
      port: parseInt(idoc.port.$)
    });
  });
};

const heartbeat = (server, app, appindex, iindex, cb) => {
  request.put(server + '/apps/:app/:instance', {
    app: app.toUpperCase(),
    instance: [[app, appindex].join('-'), iindex].join('.'),
    auth: authentication()
  }, (err, val) => err ? cb(err) : cb());
};

module.exports = server;
module.exports.server = server;
module.exports.register = register;
module.exports.deregister = deregister;
module.exports.instance = instance;
module.exports.heartbeat = heartbeat;
