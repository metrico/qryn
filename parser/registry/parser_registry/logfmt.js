const { map, addStream } = require('../common')

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
        // split
        isKey = false
        inValue = true
      } else if (line[i] === '\\') {
        i++
        value += line[i]
      } else if (line[i] === '"') {
        hadQuote = true
        inQuote = !inQuote
      } else if (line[i] !== ' ' && !inValue && !isKey) {
        isKey = true
        key = line[i]
      } else if (isKey) {
        key += line[i]
      } else if (inValue) {
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
