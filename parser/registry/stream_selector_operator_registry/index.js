const reg = require('./stream_selector_operator_registry')
const { has_extra_labels, _and } = require('../common')

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '!=': (token, query) => {
    if (query.stream) {
      return gen_stream(query, reg.eq_stream(token, query))
    }
    if (has_extra_labels(query)) {
      return _and(query, reg.neq_extra_labels(token, query))
    }
    return reg.simple_and(query, reg.neq_simple(token, query))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '=~': (token, query) => {
    if (query.stream) {
      return gen_stream(query, reg.reg_stream(token, query))
    }
    if (has_extra_labels(query)) {
      return _and(query, reg.reg_extra_labels(token, query))
    }
    return reg.simple_and(query, reg.reg_simple(token, query))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '!~': (token, query) => {
    if (query.stream) {
      return gen_stream(query, reg.nreg_stream(token, query))
    }
    if (has_extra_labels(query)) {
      return _and(query, reg.nreg_extra_labels(token, query))
    }
    return reg.simple_and(query, reg.nreg_simple(token, query))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '=': (token, query) => {
    if (query.stream) {
      return gen_stream(query, reg.eq_stream(token, query))
    }
    if (has_extra_labels(query)) {
      return _and(query, reg.eq_extra_labels(token, query))
    }
    return reg.simple_and(query, reg.eq_simple(token, query))
  }
}

/**
 *
 * @param query {registry_types.Request}
 * @param fn {function({labels: Object}): boolean}
 * @returns {registry_types.Request}
 */
const gen_stream = (query, fn) => ({
  ...query,
  stream: [...(query.stream ? query.stream : []),
    /**
         * @param stream {DataStream}
         */
    (stream) => stream.filter(fn)
  ]
})
