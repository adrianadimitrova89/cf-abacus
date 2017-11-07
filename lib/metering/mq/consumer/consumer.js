'use strict';

const amqp = require('amqplib/callback_api');
const express = require('abacus-express');
const request = require('abacus-request');
const oauth = require('abacus-oauth');

const servicesEnv = JSON.parse(process.env.VCAP_SERVICES);
const uri = servicesEnv[Object.keys(servicesEnv)[0]][0].credentials.uri;
const port = process.env.PORT || 3000;

const app = express();

const createScopesString = (scopes) => {
  return scopes && scopes.length > 0 ? scopes.join(' ') : undefined;
};

const createToken = (opts) => {
  return oauth.cache(
    opts.authServerURI,
    opts.clientId,
    opts.clientSecret,
    createScopesString(opts.scopes)
  );
};

const opts = {
  authServerURI: process.env.AUTH_SERVER,
  clientId: process.env.CLIENT_ID,
  clientSecret: process.env.CLIENT_SECRET,
  scopes: [
    'abacus.usage.linux-container.write',
    'abacus.usage.linux-container.read'
  ]
};

const dlqConfig = (dlx, key, ttl) => ({
  durable: true,
  arguments: {
    'x-dead-letter-exchange': dlx,
    'x-dead-letter-routing-key': key,
    'x-message-ttl': ttl
  }
});

app.get('/', (req, res) => {
  res.send('OK!');
});

app.listen(port, () => {
  console.log(`Consumer app listening on port ${port}!`);
});

amqp.connect(uri, (err, conn) => {
  conn.createChannel((err, ch) => {
    const workingQueue = 'abacus-queue1';
    const e = 'abacus-exchange';
    const dlx = 'dlx';
    const dlq = 'dlq';
    ch.assertExchange(e, 'direct', { durable: true });
    ch.assertQueue(workingQueue, { durable: true });

    ch.bindQueue(workingQueue, e, 'original');

    ch.assertExchange(dlx, 'direct', { durable: true });
    ch.assertQueue(dlq, dlqConfig(e, 'original', 30000));

    ch.bindQueue(dlq, dlx, 'dlq');

    ch.prefetch(1);
    console.log(' [*] Waiting for messages in %s. To exit press CTRL+C',
      workingQueue);
    ch.consume(workingQueue, (msg) => {

      console.log(' [x] Received %j', msg.content.toString());
      console.log('=====> [x] Received %j', msg);
      const tkn = createToken(opts);

      tkn.start(() => {
        request.post(':url/v1/metering/collected/usage', {
          url: 'https://demoabacus-usage-collector.cf.sap.hana.ondemand.com',
          headers: { authorization: tkn(),
            'content-type': 'application/json' },
          body: JSON.parse(msg.content.toString())
        }, (err, res) => {
          console.log('abacus responce: ', res.statusCode);
          if (res.statusCode < 300 || res.statusCode === 409)
            setImmediate(() => ch.ack(msg));
          else {
            ch.publish(dlx, 'dlq', msg.content);
            ch.ack(msg);
          }
        });
      });
    }, { noAck: false });
  });
});
