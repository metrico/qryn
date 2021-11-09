const json = require('./json')
const re = require('./regexp')
const { has_extra_labels } = require('../common')
const _i = () => { throw new Error('Not implemented') }

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  json: (token, query) => {
    if (!token.Children('parameter').length || (query.stream && query.stream.length) ||
            has_extra_labels(query)) {
      return json.via_stream(token, query)
    }
    return json.via_clickhouse_query(token, query)
  },
  logfmt: _i,

  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  regexp: (token, query) => {
    if (query.stream && query.stream.length || has_extra_labels(query)) {
      return re.via_stream(token, query)
    }
    try {
      return re.via_request(token, query)
    } catch (e) {
      console.log(e)
      return re.via_stream(token, query)
    }
  }
}
