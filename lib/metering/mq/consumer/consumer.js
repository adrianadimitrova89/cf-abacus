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

app.get('/', (req, res) => {
  res.send('OK!');
});

app.listen(port, () => {
  console.log(`Consumer app listening on port ${port}!`);
});

amqp.connect(uri, (err, conn) => {
  conn.createChannel((err, ch) => {
    const q = 'ABACUS_queue';

    ch.assertQueue(q, { durable: true });
    ch.prefetch(1);
    console.log(' [*] Waiting for messages in %s. To exit press CTRL+C', q);
    ch.consume(q, (msg) => {

      console.log(' [x] Received %s', msg.content.toString());
      const tkn = createToken(opts);

      tkn.start(() => {
        request.post(':url/v1/metering/collected/usage', {
          url: 'https://demoabacus-usage-collector.cf.sap.hana.ondemand.com',
          headers: { authorization: tkn() },
          body: msg.content.toString()
        }, (err, res) => {
          if (err)
            console.log('<<<<<<<<<ERROR>>>>>>>>>>>>> %j', err);
          else
            console.log('-----SUCCESS----- %j', res);
        });
      });
    }, { noAck: false });
  });
});
