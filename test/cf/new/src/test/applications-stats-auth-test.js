'use strict';

const statsAuthTestsDefinition = require('./test-definitions/stats-auth-test-def');
const applicationFixture = require('./fixtures/applications-bridge-fixture');

describe('applications-bridge stats auth tests', () => {

  statsAuthTestsDefinition
    .fixture(applicationFixture)
    .build();
});

