const { unquoteToken } = require('../common')
/**
 *
 * @param token {Token}
 * @returns {string[]}
 */
module.exports.labelAndVal = (token) => {
  const label = token.Child('label').value
  return [label, unquoteToken(token)]
}