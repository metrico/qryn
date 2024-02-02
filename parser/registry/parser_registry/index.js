const json = require('./json')
const re = require('./regexp')
const { hasExtraLabels, getPlugins, isEOF, hasStream, addStream } = require('../common')
const logfmt = require('./logfmt')
const drop = require('./drop')
const logger = require('../../../lib/logger')

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
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
     * @param query {Select}
     * @returns {Select}
     */
  logfmt: (token, query) => {
    return logfmt.viaStream(token, query)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  regexp: (token, query) => {
    if (hasStream(query) || hasExtraLabels(query)) {
      return re.viaStream(token, query)
    }
    try {
      return re.viaRequest(token, query)
    } catch (err) {
      logger.error({ err })
      return re.viaStream(token, query)
    }
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
  */
  drop: (token, query) => {
    if (hasStream(query)) {
      return drop.viaStream(token, query)
    }
    return drop.viaClickhouseQuery(token, query)
  },

  ...getPlugins('parser_registry', (plugin) => {
    if (plugin.map) {
      return (token, query) => {
        const mapper = plugin.map(token.Children('parameter').map(p => p.value))
        return addStream(query, (s) => s.map((e) => {
          if (!e || isEOF(e) || !e.labels || !e.string) {
            return e
          }
          return mapper(e)
        }))
      }
    }
    if (plugin.remap) {
      return (token, query) => {
        const remapper = plugin.remap(token.Children('parameter').map(p => p.value))
        return addStream(query, (s) => s.remap((emit, e) => {
          if (!e || isEOF(e) || !e.labels || !e.string) {
            emit(e)
            return
          }
          remapper(emit, e)
        }))
      }
    }
  })
}
