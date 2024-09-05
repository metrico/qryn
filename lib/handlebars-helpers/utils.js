const util = require('handlebars-utils');

let utils = {}

utils.changecase = function(str, fn) {
  if (!util.isString(str)) return '';
  if (str.length === 1) {
    return str.toLowerCase();
  }

  str = utils.chop(str).toLowerCase();
  if (typeof fn !== 'function') {
    fn = utils.identity;
  }

  var re = /[-_.\W\s]+(\w|$)/g;
  return str.replace(re, function(_, ch) {
    return fn(ch);
  });
};

/**
 * Generate a random number
 *
 * @param {Number} `min`
 * @param {Number} `max`
 * @return {Number}
 * @api public
 */

utils.random = function(min, max) {
  return min + Math.floor(Math.random() * (max - min + 1));
};

utils = {
  ...utils,
  ...require('is-number')
};

utils.chop = function(str) {
  if (!util.isString(str)) return '';
  var re = /^[-_.\W\s]+|[-_.\W\s]+$/g;
  return str.trim().replace(re, '');
};

/**
 * Expose `utils`
 */

module.exports = utils;
