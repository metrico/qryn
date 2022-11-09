const { unquoteToken } = require('./common')
const Sql = require('@cloki/clickhouse-sql')

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  '|=': (token, query) => {
    const val = unquoteToken(token)
    if (!val) {
      return query
    }
    query.where(Sql.Ne(new Sql.Raw(`position(string, '${val}')`), 0))
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
    if (!val) {
      return query
    }
    query.where(Sql.Ne(new Sql.Raw(`extractAllGroups(string, '(${val})')`), new Sql.Raw('[]')))
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
    query.where(Sql.Eq(new Sql.Raw(`position(string, '${val}')`), 0))
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
    query.where(Sql.Eq(new Sql.Raw(`extractAllGroups(string, '(${val})')`), new Sql.Raw('[]')))
    return query
  }
}
