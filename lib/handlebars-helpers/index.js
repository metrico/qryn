/*!
 * handlebars-helpers <https://github.com/helpers/handlebars-helpers>
 *
 * Copyright (c) 2013-2017, Jon Schlinkert, Brian Woodward.
 * Released under the MIT License.
 */

'use strict';

var lib = {
  math: require('./math'),
  string: require('./string'),
}

/**
 * Expose helpers
 */

module.exports = function helpers(groups, options) {
  if (typeof groups === 'string') {
    groups = [groups];
  } else if (!Array.isArray(groups)) {
    options = groups;
    groups = null;
  }

  options = options || {};
  const hbs = options.handlebars || options.hbs || require('handlebars');
  module.exports.handlebars = hbs;

  if (groups) {
    groups.forEach(function(key) {
      hbs.registerHelper(lib[key]);
    });
  } else {
    Object.values(lib).forEach(function(group) {
      hbs.registerHelper(group);
    });
  }

  return hbs.helpers;
};

/**
 * Expose helper groups
 */

Object.entries(lib).forEach(function(key_group) {
  const [key, group] = key_group;
  module.exports[key] = function(options) {
    options = options || {};
    let hbs = options.handlebars || options.hbs || require('handlebars');
    module.exports.handlebars = hbs;
    hbs.registerHelper(group);
    return hbs.helpers;
  };
});
