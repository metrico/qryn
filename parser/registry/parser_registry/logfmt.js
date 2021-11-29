const { Compiler } = require('bnf/Compiler')
const { _and, map } = require('../common')
const debug = true
/**
 *
 * @type {function(Token): Object | undefined}
 */
const getLabels = (() => {
  const compiler = new Compiler()
  compiler.AddLanguage(`
    <SYNTAX> ::= first_part *(part)
    <first_part> ::= 1*(<ALPHA> | "_" | <DIGITS>)
    <part> ::= ("." <first_part>) | "[" <QLITERAL> "]" | "[" <DIGITS> "]"
      `, 'logfmt')
  /**
  * @param token {Token}
  * @returns {Object | undefined}
  */
  return (token) => {
    if (debug)console.log('logfmt: testing1')
    if (!token.Children('parameter').length) {
      return undefined
    }
    return token.Children('parameter').reduce((sum, p) => {
      const label = p.Child('label').value
      if (debug)console.log('logfmt: getting', label, sum, p)
      let val = compiler.ParseScript(JSON.parse(p.Child('quoted_str').value))
      val = [
        val.rootToken.Child('first_part').value,
        ...val.rootToken.Children('part').map(t => t.value)
      ]
      sum[label] = val
      return sum
    }, {})
  }
})()

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.viaClickhouseQuery = (token, query) => {
  if (debug)console.log('logfmt: testing2')
  const labels = getLabels(token)
  let exprs = Object.entries(labels).map(lbl => {
    const path = lbl[1].map(path => {
      if (path.startsWith('.')) {
        return `'${path.substring(1)}'`
      }
      if (path.startsWith('["')) {
        return `'${JSON.parse(path.substring(1, path.length - 1))}'`
      }
      if (path.startsWith('[')) {
        return (parseInt(path.substring(1, path.length - 1)) + 1).toString()
      }
      return `'${path}'`
    })
    const expr = `if(JSONType(samples.string, ${path.join(',')}) == 'String', ` +
            `JSONExtractString(samples.string, ${path.join(',')}), ` +
            `JSONExtractRaw(samples.string, ${path.join(',')}))`
    return `('${lbl[0]}', ${expr})`
  })
  exprs = "arrayFilter((x) -> x.2 != '', [" + exprs.join(',') + '])'
  return _and({
    ...query,
    select: [...query.select.filter(f => !f.endsWith('as extra_labels')), `${exprs} as extra_labels`]
  }, ['isValidJSON(samples.string)'])
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.viaStream = (token, query) => {
  const labels = getLabels(token)

  if (debug)console.log('logfmt: testing4 - undefined', labels)

  /**
    *
    * @param {string} line
    */

  const extractLabels = (line) => {
    let key = ''
    let value = ''
    let isNumber = true
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
    if (debug)console.log('logfmt: testing5', stream)
    return map(stream, (e) => {
      if (debug)console.log('logfmt: testing6 - e', e)
      if (debug)console.log('logfmt: testing7 - data', e.string)
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
  return {
    ...query,
    stream: [...(query.stream ? query.stream : []), stream]
  }
}
