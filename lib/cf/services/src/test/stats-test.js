'use strict';

const request = require('abacus-request');
const jwt = require('jsonwebtoken');

const decodedToken = require('./token.json');

const buildStatsPayload = (secured) => {
  return {
    services: {
      config: {
        secured: secured,
        minIntervalTime: 5000,
        maxIntervalTime: 240000,
        guidMinAge: 60000,
        reporting: {
          minInterval: 5000,
          maxInterval: 240000,
          guidMinAge: 60000,
          maxRetries: 12,
          currentRetries: 0
        }
      },
      cache: {},
      performance: {
        cache: {
          read: {
            name: 'cache.read',
            counts: [{
              ok: 0,
              errors: 0,
              timeouts: 0,
              rejects: 0
            }],
            latencies: [{
              latencies: []
            }],
            health: [{
              ok: 0,
              errors: 0
            }],
            circuit: 'closed'
          },
          write: {
            name: 'cache.write',
            counts: [{
              ok: 0,
              errors: 0,
              timeouts: 0,
              rejects: 0
            }],
            latencies: [{
              latencies: []
            }],
            health: [{
              ok: 0,
              errors: 0
            }],
            circuit: 'closed'
          }
        },
        paging: {
          pages: {
            name: 'paging',
            counts: [{
              ok: 0,
              errors: 0,
              timeouts: 0,
              rejects: 0
            }],
            latencies: [{
              latencies: []
            }],
            health: [{
              ok: 0,
              errors: 0
            }],
            circuit: 'closed'
          },
          resources: {
            name: 'paging.resources',
            counts: [{
              ok: 0,
              errors: 0,
              timeouts: 0,
              rejects: 0
            }],
            latencies: [{
              latencies: []
            }],
            health: [{
              ok: 0,
              errors: 0
            }],
            circuit: 'closed'
          }
        },
        report: {
          name: 'report',
          counts: [{
            ok: 0,
            errors: 0,
            timeouts: 0,
            rejects: 0
          }],
          latencies: [{
            latencies: []
          }],
          health: [{
            ok: 0,
            errors: 0
          }],
          circuit: 'closed'
        },
        usage: {
          name: 'usage',
          counts: [{
            ok: 0,
            errors: 0,
            timeouts: 0,
            rejects: 0
          }],
          latencies: [{
            latencies: []
          }],
          health: [{
            ok: 0,
            errors: 0
          }],
          circuit: 'closed'
        },
        carryOver: {
          circuit: 'closed',
          counts: [{
            errors: 0,
            ok: 0,
            rejects: 0,
            timeouts: 0
          }],
          health: [{
            errors: 0,
            ok: 0
          }],
          latencies: [{
            latencies: []
          }],
          name: 'carryOver'
        }
      },
      statistics: {
        cache: {
          readSuccess: 0,
          readFailure: 0,
          writeSuccess: 0,
          writeFailure: 0
        },
        usage: {
          missingToken: 0,
          reportFailures: 0,
          reportSuccess: 0,
          reportBusinessError: 0,
          reportConflict: 0,
          loopFailures: 0,
          loopSuccess: 0,
          loopConflict: 0,
          loopSkip: 0
        },
        carryOver: {
          getSuccess: 0,
          getNotFound: 0,
          getFailure: 0,
          removeFailure: 0,
          removeSuccess: 0,
          upsertFailure: 0,
          upsertSuccess: 0,
          docsRead: 0,
          readSuccess: 0,
          readFailure: 0
        },
        paging: {
          missingToken: 0,
          pageReadSuccess: 0,
          pageReadFailures: 0,
          pageProcessSuccess: 0,
          pageProcessFailures: 0,
          pageProcessEnd: 0
        }
      },
      errors: {
        missingToken: false,
        noReportEverHappened: true,
        consecutiveReportFailures: 0,
        lastError: '',
        lastErrorTimestamp: ''
      }
    }
  };
};

describe('service bridge statistics', () => {
  let server;
  let clientToken;

  const startBridge = () => {
    delete require.cache[require.resolve('..')];

    process.env.CLUSTER = 'false';
    const service = require('..');
    const app = service();
    server = app.listen(0);
  };

  const stopBridge = () => {
    server.close();
  };

  const callStatsEndpoint = (cb) => {
    const headers = {};
    if (clientToken)
      headers.authorization = 'bearer ' + clientToken;

    request.get('http://localhost::port/v1/stats', {
      port: server.address().port,
      headers: headers
    }, (err, resp) => {
      cb(err, resp);
    });
  };

  const deleteTimeStamps = (object) => {
    for (const key in object) {
      if (key === 'i' || key === 'time')
        delete object[key];
      if (object[key] !== null && typeof object[key] === 'object')
        deleteTimeStamps(object[key]);
    }
  };

  beforeEach(() => {
    clientToken = undefined;
  });

  context('when running unsecured', () => {
    beforeEach(() => {
      delete process.env.SECURED;
      delete process.env.JWTKEY;
      delete process.env.JWTALGO;

      startBridge();
    });

    afterEach(() => {
      stopBridge();
    });

    it('returns stats', (done) => {
      callStatsEndpoint((err, resp) => {
        expect(err).to.equal(undefined);
        expect(resp.statusCode).to.equal(200);

        const responseBody = resp.body;
        deleteTimeStamps(responseBody);
        expect(responseBody).to.deep.equal(buildStatsPayload(false));

        done();
      });
    });
  });

  context('when running secured', () => {
    const tokenSecret = 'secret';
    const tokenAlgorithm = 'HS256';

    beforeEach(() => {
      process.env.SECURED = 'true';
      process.env.JWTKEY = tokenSecret;
      process.env.JWTALGO = tokenAlgorithm;
    });

    context('client calls without a token', () => {
      beforeEach(() => {
        startBridge();
      });

      afterEach(() => {
        stopBridge();
      });

      it('returns unauthorized', (done) => {
        callStatsEndpoint((err, resp) => {
          expect(err).to.equal(undefined);
          expect(resp.statusCode).to.equal(401);
          done();
        });
      });
    });

    context('valid token is provided', () => {
      beforeEach(() => {
        clientToken = jwt.sign(decodedToken, tokenSecret, {
          algorithm: tokenAlgorithm,
          expiresIn: 43200
        });

        startBridge();
      });

      afterEach(() => {
        stopBridge();
      });

      it('returns stats', (done) => {
        callStatsEndpoint((err, resp) => {
          expect(err).to.equal(undefined);
          expect(resp.statusCode).to.equal(200);

          const responseBody = resp.body;
          deleteTimeStamps(responseBody);
          expect(responseBody).to.deep.equal(buildStatsPayload(true));

          done();
        });
      });
    });
  });
});

