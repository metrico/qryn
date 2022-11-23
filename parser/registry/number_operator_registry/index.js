const aggReg = require('./compared_agg_reg')
const labelReg = require('./compared_label_reg')

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @param aggregatedProcessor {(function(Token, Select): Select)}
 * @param labelComparer {(function(Token, Select): Select)}
 * @returns {Select}
 */
function genericReq (token, query,
  aggregatedProcessor, labelComparer) {
  if ((token.name === 'agg_statement' || token.Child('agg_statement')) &&
    token.Child('compared_agg_statement_cmp')) {
    return aggregatedProcessor(token, query)
  }
  if (token.name === 'number_label_filter_expression' || token.Child('number_label_filter_expression')) {
    return labelComparer(token, query)
  }
  throw new Error('Not implemented')
}

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '==': (token, query) => {
    return genericReq(token, query, aggReg.eq, labelReg.eq)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '>': (token, query) => {
    return genericReq(token, query, aggReg.gt, labelReg.gt)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '>=': (token, query) => {
    return genericReq(token, query, aggReg.ge, labelReg.ge)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '<': (token, query) => {
    return genericReq(token, query, aggReg.lt, labelReg.lt)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '<=': (token, query) => {
    return genericReq(token, query, aggReg.le, labelReg.le)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '!=': (token, query) => {
    return genericReq(token, query, aggReg.neq, labelReg.neq)
  }
}
