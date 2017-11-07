'use strict';

const amqp = require('amqplib/callback_api');
const bodyParser = require('body-parser');
const express = require('abacus-express');

const app = express();
app.use(bodyParser.json());

const servicesEnv = JSON.parse(process.env.VCAP_SERVICES);
const uri = servicesEnv[Object.keys(servicesEnv)[0]][0].credentials.uri;
const port = process.env.PORT || 3000;

app.post('/usage', (req, res) => {
  console.log('New task');
  amqp.connect(uri, (err, conn) => {
    conn.createChannel((err, ch) => {
      // const qName = 'abacus-queue';
      const exchangeName = 'abacus-exchange';
      const bindingKey = 'original';
      const msg = req.body;
      ch.publish(exchangeName, bindingKey, new Buffer(JSON.stringify(msg)));
      console.log('==========>', msg);
      // ch.assertQueue(qName, { durable: true });
      // const resp = ch.sendToQueue(qName, new Buffer(JSON.stringify(msg)),
      //   { persistent: true });
      // console.log('sendToQueue: %j', resp);

      res.status(202).send();
    });
  });

});

app.listen(port, () => {
  console.log(`Producer app listening on port ${port}!`);
});

