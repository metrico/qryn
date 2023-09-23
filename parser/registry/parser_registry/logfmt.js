const { map, addStream } = require('../common')
const logfmt = require('logfmt')

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.viaStream = (token, query) => {
  /**
    *
    * @param {string} line
    */

  const extractLabels = (line) => {
    const labels = logfmt.parse(line)
    return Object.fromEntries(Object.entries(labels).filter(([k, v]) => typeof v === 'string'))
  }

  /**
     *
     * @param {DataStream} stream
     * @return {DataStream}
     */
  const stream = (stream) => {
    return map(stream, (e) => {
      if (!e || !e.labels) {
        return { ...e }
      }

      try {
        const extraLabels = extractLabels(e.string)
        return { ...e, labels: { ...e.labels, ...extraLabels } }
      } catch (err) {
        return undefined
      }
    })
  }
  return addStream(query, stream)
}
