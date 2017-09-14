'use strict';

const expGeneratorCreator = require('../exponential-generator');

describe('service-bridge/dbcache', () => {
  const startValue = 10;
  const maxValue = 20;
  let generator;

  beforeEach(() => {
    generator = expGeneratorCreator.create(startValue, maxValue);
  });

  context('when invoked ', () => {
    let first;
    let second;
    let third;

    beforeEach(() => {
      first = generator.getNext();
      second = generator.getNext();
      third = generator.getNext();
    });

    it('expect exponential values are returned', () => {
      expect(first).to.equal(startValue);
      expect(second).to.equal(startValue + 1);
      expect(third).to.equal(startValue + 6);
    });

    context('when next value is above max value', () => {

      it('expect max value is returned', () => {
        expect(generator.getNext()).to.equal(maxValue);
      });

    });

    context('when reset ', () => {

      it('expect start value is returned', () => {
        generator.reset();
        expect(generator.getNext()).to.equal(startValue);
      });

    });

  });

});
