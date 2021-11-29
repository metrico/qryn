const json = require('./json')
const re = require('./regexp')
const logfmt = require('./logfmt')
const { hasExtraLabels, getPlugins, isEOF } = require('../common')
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

  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  logfmt: (token, query) => {
    if (!token.Children('parameter').length || (query.stream && query.stream.length) ||
            hasExtraLabels(query)) {
      return logfmt.viaStream(token, query)
    }
    return _i // logfmt.viaClickhouseQuery(token, query)
  },

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
  },
  ...getPlugins('parser_registry', (plugin) => {
    if (plugin.map) {
      return (token, query) => {
        const mapper = plugin.map(token.Children('parameter').map(p => p.value))
        return {
          ...query,
          stream: [
            ...(query.stream || []),
            (s) => s.map((e) => {
              if (!e || isEOF(e) || !e.labels || !e.string) {
                return e
              }
              return mapper(e)
            })
          ]
        }
      }
    }
    if (plugin.remap) {
      return (token, query) => {
        const remapper = plugin.remap(token.Children('parameter').map(p => p.value))
        return {
          ...query,
          stream: [
            ...(query.stream || []),
            (s) => s.remap((emit, e) => {
              if (!e || isEOF(e) || !e.labels || !e.string) {
                emit(e)
                return
              }
              remapper(emit, e)
            })
          ]
        }
      }
    }
  })
}
