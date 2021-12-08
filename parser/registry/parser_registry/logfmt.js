const { map, addStream } = require('../common')
const debug = false

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
    let key = ''
    let value = ''
    let isKey = false
    let inValue = false
    let inQuote = false
    let hadQuote = false
    const object = {}

    if (line[line.length - 1] === '\n') {
      line = line.slice(0, line.length - 1)
    }

    for (let i = 0; i <= line.length; i++) {
      if ((line[i] === ' ' && !inQuote) || i === line.length) {
        if (isKey && key.length > 0) {
          object[key] = true
        } else if (inValue) {
          if (value === 'true') value = true
          else if (value === 'false') value = false
          else if (value === '' && !hadQuote) value = null
          object[key] = value
          value = ''
        }

        if (i === line.length) break
        else {
          isKey = false
          inValue = false
          inQuote = false
          hadQuote = false
        }
      }

      if (line[i] === '=' && !inQuote) {
        if (debug) console.log('split')
        // split
        isKey = false
        inValue = true
      } else if (line[i] === '\\') {
        i++
        value += line[i]
        if (debug) console.log('escape: ' + line[i])
      } else if (line[i] === '"') {
        hadQuote = true
        inQuote = !inQuote
        if (debug) console.log('in quote: ' + inQuote)
      } else if (line[i] !== ' ' && !inValue && !isKey) {
        if (debug) console.log('start key with: ' + line[i])
        isKey = true
        key = line[i]
      } else if (isKey) {
        if (debug) console.log('add to key: ' + line[i])
        key += line[i]
      } else if (inValue) {
        if (debug) console.log('add to value: ' + line[i])
        value += line[i]
      }
    }

    return object
  }

  /**
     *
     * @param {DataStream} stream
     * @return {DataStream}
     */
  const stream = (stream) => {
    if (debug) console.log('logfmt: on receipt of stream', stream)
    return map(stream, (e) => {
      if (debug) console.log('logfmt: stream.e ', e)
      if (debug) console.log('logfmt: stream.data ', e.string)
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
