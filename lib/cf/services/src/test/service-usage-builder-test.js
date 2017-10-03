'use strict';

const states = require('../service-event-states');
const serviceUsageBuilder = require('../service-usage-builder');

describe('service-bridge/service-usage-builder', () => {
  const sandbox = sinon.sandbox.create();

  let adjustTimestampStub;
  let isSupportedStub;
  let builder;

  beforeEach(() => {
    adjustTimestampStub = sandbox.stub();
    const carryOver = {
      adjustTimestamp: adjustTimestampStub
    };
    isSupportedStub = sandbox.stub();
    const checkerStub = {
      isSupported: isSupportedStub
    };

    builder = serviceUsageBuilder.create(
      carryOver, checkerStub);
  });

  afterEach(() => {
    sandbox.restore();
  });

  context('when no event is provided', () => {

    it('should callback with no parameters', (done) => {
      builder.buildServiceUsage(undefined, (err, usage) => {
        expect(err).to.not.equal(undefined);
        done();
      });
    });

  });

  context('when event is provided', () => {
    const serviceGuid = 'service-guid';
    const createEvent = (state) => ({
      metadata: {
        created_at: 1000,
        guid: serviceGuid
      },
      entity: {
        state,
        org_guid: 'org-guid',
        space_guid: 'space-guid',
        service_label: 'label',
        service_plan_name: 'plan-name',
        service_instance_guid: 'service-instance-guid'
      }
    });

    context('when event is supported', () => {
      let actualUsage;
      let actualErr;

      const createUsage = (current, previous) => ({
        start: 1000,
        end: 1000,
        organization_id: 'org-guid',
        space_id: 'space-guid',
        consumer_id: 'service:service-instance-guid',
        resource_id: 'label',
        plan_id: 'plan-name',
        resource_instance_id: 'service:service-instance-guid:plan-name:label',
        measured_usage: [{
          measure: 'current_instances',
          quantity: current
        },{
          measure: 'previous_instances',
          quantity: previous
        }]
      });

      beforeEach(() => {
        isSupportedStub.returns(true);
        adjustTimestampStub.callsFake((doc, eventGuid, cb) => {
          cb(undefined, doc);
        });
      });

      const itTimestampAdjusted = () => {
        it('adjustTimestamp should be called', () => {
          assert.calledOnce(adjustTimestampStub);
          assert.calledWithExactly(adjustTimestampStub,
            sinon.match.any,
            serviceGuid,
            sinon.match.any);
        });
      };

      context('when CREATED event is provided', () => {
        beforeEach((done) => {
          const event = createEvent(states.CREATED);
          builder.buildServiceUsage(event, (err, usage) => {
            actualErr = err;
            actualUsage = usage;
            done();
          });
        });

        it('should callback with correct usage', () => {
          expect(actualErr).to.equal(undefined);
          expect(actualUsage).to.deep.equal(createUsage(1, 0));
        });

        itTimestampAdjusted();
      });

      context('when DELETED event is provided', () => {
        beforeEach((done) => {
          const event = createEvent(states.DELETED);
          builder.buildServiceUsage(event, (err, usage) => {
            actualErr = err;
            actualUsage = usage;
            done();
          });
        });

        it('should callback with correct usage', () => {
          expect(actualErr).to.equal(undefined);
          expect(actualUsage).to.deep.equal(createUsage(0, 1));
        });

        itTimestampAdjusted();
      });
    });

    context('when unsupported event is provided', () => {
      beforeEach(() => {
        isSupportedStub.returns(false);
      });

      it('should callback with "unsupportedEventError" ', (done) => {
        const event = createEvent(states.UPDATED);
        builder.buildServiceUsage(event, (err, usage) => {
          expect(err).to.equal(serviceUsageBuilder.unsupportedEventError);
          expect(usage).to.equal(undefined);
          done();
        });
      });
    });

  });
});