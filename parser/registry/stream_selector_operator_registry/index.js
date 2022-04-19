const reg = require('./stream_selector_operator_registry')
const idxReg = require('./stream_selector_indexed_registry')
const { hasExtraLabels } = require('../common')

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '!=': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.eqStream(token))
    }
    if (hasExtraLabels(query)) {
      return query.where(reg.neqExtraLabels(token))
    }
    if (query.ctx.legacy) {
      return reg.simpleAnd(query, reg.neqSimple(token))
    }
    return idxReg.indexedAnd(query, idxReg.neqIndexed(token))
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '=~': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.regStream(token))
    }
    if (hasExtraLabels(query)) {
      return query.where(query, reg.regExtraLabels(token))
    }
    if (query.ctx.legacy) {
      return reg.simpleAnd(query, reg.regSimple(token))
    }
    return idxReg.indexedAnd(query, idxReg.reIndexed(token))
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '!~': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.nregStream(token))
    }
    if (hasExtraLabels(query)) {
      return query.where(query, reg.nregExtraLabels(token))
    }
    if (query.ctx.legacy) {
      return reg.simpleAnd(query, reg.nregSimple(token))
    }
    return idxReg.indexedAnd(query, idxReg.nreIndexed(token))
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '=': (token, query) => {
    if (query.stream) {
      return genStream(query, reg.eqStream(token))
    }
    if (hasExtraLabels(query)) {
      return query.where(query, reg.eqExtraLabels(token))
    }
    if (query.ctx.legacy) {
      return reg.simpleAnd(query, reg.eqSimple(token))
    }
    return idxReg.indexedAnd(query, idxReg.eqIndexed(token))
  }
}

/**
 *
 * @param query {Select}
 * @param fn {function({labels: Object}): boolean}
 * @returns {Select}
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
