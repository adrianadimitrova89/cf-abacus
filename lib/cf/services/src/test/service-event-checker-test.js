'use strict';

const checkerCreator = require('../service-event-checker');
const states = require('../service-event-states');

describe('service-bridge/service-event-checker', () => {
  let checker;

  context('with NO preconfigured supported services', () => {

    it('"false" is expected', () => {
      checker = checkerCreator.create();

      const supported = checker.isSupported({
        entity:{
          state: states.CREATED,
          service_label: 'mongodb',
          service_plan_name: 'small'
        } });

      expect(supported).to.be.equal(false);
    });

  });

  context('with preconfigured supported services', () => {

    const services = {
      mongodb: {
        plans: ['small'],
        guid: 'some-guid'
      }
    };

    beforeEach(() => {
      checker = checkerCreator.create(services);
    });

    context('when supported event is provided', () => {

      context('with CREATED state', () => {

        it('"true" is expected', () => {
          const supported = checker.isSupported({
            entity:{
              state: states.CREATED,
              service_label: 'mongodb',
              service_plan_name: 'small'
            } });

          expect(supported).to.be.equal(true);
        });
      });

      context('with DELETED state', () => {

        it('"true" is expected', () => {
          const supported = checker.isSupported({
            entity:{
              state: states.DELETED,
              service_label: 'mongodb',
              service_plan_name: 'small'
            } });

          expect(supported).to.be.equal(true);
        });
      });

    });

    context('when unsupported event is provided', () => {

      context('unsupported state', () => {

        it('"false" is expected', () => {
          const supported = checker.isSupported({
            entity:{
              state: states.UPDATED,
              service_label: 'mongodb',
              service_plan_name: 'small'
            } });

          expect(supported).to.be.equal(false);
        });
      });

      context('unsupported service', () => {

        it('"false" is expected', () => {
          const supported = checker.isSupported({
            entity:{
              state: states.CREATED,
              service_label: 'postgre',
              service_plan_name: 'small'
            } });

          expect(supported).to.be.equal(false);
        });
      });

      context('unsupported plan', () => {

        it('"false" is expected', () => {
          const supported = checker.isSupported({
            entity:{
              state: states.CREATED,
              service_label: 'mongodb',
              service_plan_name: 'large'
            } });

          expect(supported).to.be.equal(false);
        });
      });
    });

  });

});
