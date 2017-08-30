'use strict';

describe('abacus-cf-services', () => {

  context('when services configuration is invalid', () => {
    beforeEach(() => {
      process.env.SERVICES = 'invalid_json';
    });

    it('should throw an error', () => {
      delete require.cache[require.resolve('..')];
      expect(() => require('..')).to.throw();
    });
  });

});
