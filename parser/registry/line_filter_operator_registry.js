const { _and, unquote_token, querySelectorPostProcess } = require('./common')

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '|=': (token, query) => {
    const val = unquote_token(token)
    return querySelectorPostProcess(_and(query, [
            `position(string, '${val}') != 0`
    ]))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '|~': (token, query) => {
    const val = unquote_token(token)
    return querySelectorPostProcess(_and(query, [
            `extractAllGroups(string, '(${val})') != []`
    ]))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '!=': (token, query) => {
    const val = unquote_token(token)
    return querySelectorPostProcess(_and(query, [
            `position(string, '${val}') == 0`
    ]))
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  '!~': (token, query) => {
    const val = unquote_token(token)
    return querySelectorPostProcess(_and(query, [
            `extractAllGroups(string, '(${val})') == []`
    ]))
  }
}
