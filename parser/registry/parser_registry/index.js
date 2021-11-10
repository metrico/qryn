const json = require('./json')
const re = require('./regexp')
const { hasExtraLabels } = require('../common')
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
            hasExtraLabels(query)) {
      return json.viaStream(token, query)
    }
    return json.viaClickhouseQuery(token, query)
  },
  logfmt: _i,

  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  regexp: (token, query) => {
    if ((query.stream && query.stream.length) || hasExtraLabels(query)) {
      return re.viaStream(token, query)
    }
    try {
      return re.viaRequest(token, query)
    } catch (e) {
      console.log(e)
      return re.viaStream(token, query)
    }
  }
}
