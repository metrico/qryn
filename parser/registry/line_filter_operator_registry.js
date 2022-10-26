const { unquoteToken } = require('./common')
const Sql = require('@cloki/clickhouse-sql')

/**
 * @param val {string}
 * @returns {string}
 */
const likePercent = (val) => {
  if (!val) {
    return "''"
  }
  val = Sql.quoteVal(val).toString()
  val = val.substring(1, val.length - 1)
  val = val.replace(/([%_])/g, '\\$1')
  return `'%${val}%'`
}

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '|=': (token, query) => {
    const val = unquoteToken(token)
    query.where(Sql.Ne(new Sql.Raw(`like(string, ${likePercent(val)})`), 0))
    return query
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '|~': (token, query) => {
    const val = unquoteToken(token)
    query.where(Sql.Eq(new Sql.Raw(`match(string, ${Sql.quoteVal(val)})`), new Sql.Raw('1')))
    return query
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '!=': (token, query) => {
    const val = unquoteToken(token)
    query.where(Sql.Eq(new Sql.Raw(`notLike(string, ${likePercent(val)})`), 1))
    return query
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '!~': (token, query) => {
    const val = unquoteToken(token)
    query.where(Sql.Eq(new Sql.Raw(`match(string, ${Sql.quoteVal(val)})`), new Sql.Raw('0')))
    return query
  }
}
