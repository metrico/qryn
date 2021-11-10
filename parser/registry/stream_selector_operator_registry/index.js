const reg = require('./stream_selector_operator_registry')
const { hasExtraLabels, _and } = require('../common')

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '!=': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.eqStream(token, query))
    }
    if (hasExtraLabels(query)) {
      return _and(query, reg.neqExtraLabels(token, query))
    }
    return reg.simpleAnd(query, reg.neqSimple(token, query))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '=~': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.regStream(token, query))
    }
    if (hasExtraLabels(query)) {
      return _and(query, reg.regExtraLabels(token, query))
    }
    return reg.simpleAnd(query, reg.regSimple(token, query))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '!~': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.nregStream(token, query))
    }
    if (hasExtraLabels(query)) {
      return _and(query, reg.nregExtraLabels(token, query))
    }
    return reg.simpleAnd(query, reg.nregSimple(token, query))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '=': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.eqStream(token, query))
    }
    if (hasExtraLabels(query)) {
      return _and(query, reg.eqExtraLabels(token, query))
    }
    return reg.simpleAnd(query, reg.eqSimple(token, query))
  }
}

/**
 *
 * @param query {registry_types.Request}
 * @param fn {function({labels: Object}): boolean}
 * @returns {registry_types.Request}
 */
const genStream = (query, fn) => ({
  ...query,
  stream: [...(query.stream ? query.stream : []),
    /**
         * @param stream {DataStream}
         */
    (stream) => stream.filter(fn)
  ]
})
