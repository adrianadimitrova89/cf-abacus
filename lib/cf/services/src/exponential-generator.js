'use strict';


const create = (startValue, maxValue) => {
  let count = 0;

  return {
    getNext: () => {
      const next = startValue + Math.floor(Math.expm1(count));
      count++;
      if (next > maxValue)
        return maxValue;

      return next;
    },
    reset: () => {
      count = 0;
    }
  };

};

module.exports.create = create;
