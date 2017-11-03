'use strict';

const amqp = require('amqplib/callback_api');
const bodyParser = require('body-parser');
const express = require('abacus-express');

const app = express();
app.use(bodyParser.json());

const servicesEnv = JSON.parse(process.env.VCAP_SERVICES);
const uri = servicesEnv[Object.keys(servicesEnv)[0]][0].credentials.uri;
const port = process.env.PORT || 3000;

let counter = 1;
app.post('/usage', (req, res) => {
  console.log('New task');
  amqp.connect(uri, (err, conn) => {
    console.log('Task %d', counter);
    conn.createChannel((err, ch) => {
      const qName = 'ABACUS_queue';
      const msg = req.body;

      ch.assertQueue(qName, { durable: true });
      ch.sendToQueue(qName, new Buffer(JSON.stringify(msg)),
        { persistent: true });
      res.status(202).send();
    });
  });

});

app.listen(port, () => {
  console.log(`Producer app listening on port ${port}!`);
});

