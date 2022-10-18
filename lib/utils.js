const logger = require('./logger')
const stableStringify = require('json-stable-stringify')
/* Function Helpers */

/* Label Parser */
const labelParser = function (labels) {
  // Label Parser
  const rx = /\"?\b(\w+)\"?(!?=~?)("[^"\n]*?")/g
  let matches
  const output = []
  matches = rx.exec(labels)
  while (matches) {
    if (matches.length > 3) output.push([matches[1], matches[2], matches[3].replace(/['"]+/g, '')])
    matches = rx.exec(labels)
  }
  let regex = false
  try {
    regex = /\}\s*(.*)/g.exec(labels)[1] || false
  } catch (e) {
  }
  return { labels: output, regex: regex }
}
/* Fingerprinting */
const shortHash = require('short-hash')
let xxh = null
require('xxhash-wasm')().then((res) => {
  xxh = res
  logger.info('xxh ready')
}, (err) => {
  logger.error(err)
  process.exit(1)
})
const fingerPrint = function (text, hex, alg) {
  alg = alg || process.env.HASH || 'xxhash64'
  switch (alg) {
    case 'xxhash64':
      if (!xxh) {
        throw new Error('Hasher is not ready')
      }
      if (hex) return xxh.h64(text).toString()
      else return BigInt('0x' + xxh.h64(text))
  }
  if (hex) return shortHash(text)
  else return parseInt(shortHash(text), 16)
}

/**
 *
 * @param str {string}
 * @returns Object
 */
const toJSON = (function () {
  const labelsLang = `
<SYNTAX> ::= ["{"] <OWSP> <pair> <OWSP> *("," <OWSP> <pair>) <OWSP> ["}"]
label ::= (<ALPHA> | "_") *(<ALPHA> | "." | "_" | <DIGITS>)
val ::= <QLITERAL> | <label>
pair ::= <label> <OWSP> "=" <OWSP> <val>
`
  const Compiler = require('bnf/Compiler').Compiler
  const compiler = new Compiler()
  compiler.AddLanguage(labelsLang, 'labels')
  const getPairs = (root) => {
    let tokens = []
    for (let i = 0; i < root.tokens.length; i++) {
      if (root.tokens[i].name === 'pair') {
        tokens.push(root.tokens[i])
      }
      tokens = [...tokens, ...getPairs(root.tokens[i])]
    }

    return tokens
  }
  return (str) => {
    const script = compiler.ParseScript(str)
    const pairs = getPairs(script.rootToken)
    const res = {}
    for (const pair of pairs) {
      let val = pair.Child('val')
      val = val.Child('QLITERAL')
        ? JSON.parse(val.value)
        : val.value
      res[pair.Child('label').value] = val
    }
    return res
  }
})() //require('jsonic')

const parseOrDefault = (str, def) => {
  try {
    return str ? parseFloat(str) : def
  } catch (e) {
    return def
  }
}

/**
 * @param str {String}
 * @param def {Number}
 * @return {Number} duration in sec or default
 */
const parseDurationSecOrDefault = (str, def) => {
  const multiplier = {
    ns: 1e9,
    us: 1e6,
    ms: 1e3,
    s: 1,
    m: 1 / 60,
    h: 1 / 3600
  }
  if (!str) {
    return def
  }
  const match = str.toString().match(/^(?<num>[0-9.]+)(?<unit>ns|us|ms|s|m|h)?$/)
  if (!match) {
    return def
  }
  const unit = match.groups.unit || 's'
  const num = parseFloat(match.groups.num)
  return num / multiplier[unit]
}

const parseMs = (time, def) => {
  try {
    return time ? Math.floor(parseInt(time) / 1000000) : def
  } catch (e) {
    return def
  }
}

/**
 *
 * @param time {string | BigInt}
 * @return {BigInt | undefined}
 */
const parseStringifiedNanosOrRFC3339 = (time) => {
  if (typeof time === 'bigint') {
    return time
  }
  const iMatch = time.match(/^[0-9]+$/)
  if (iMatch) {
    // It is nanos
    return BigInt(time)
  }
  const dMatch = time.match(/(?<y>[0-9]{4})-(?<m>[0-1][0-9])-(?<d>[0-3][0-9])T(?<h>[0-2][0-9]):(?<i>[0-6][0-9]):(?<s>[0-6][0-9])(?<ns>\.[0-9]+)?(?<offs>Z|(\+|-)(?<oh>[0-2][0-9]):(?<oi>[0-6][0-9]))/)
  if (dMatch) {
    const g = dMatch.groups
    let iTime = Date.UTC(g.y, parseInt(g.m) - 1, g.d, g.h, g.i, g.s)
    if (g.offs !== 'Z') {
      iTime += (g.offs[0] === '+' ? 1 : -1) * (parseInt(g.oh) * 3600 * 1000 + parseInt(g.oi) * 60 * 1000)
    }
    const ns = g.ns ? BigInt((g.ns + '000000000').substr(1, 9)) : BigInt(0)
    return BigInt(iTime) * BigInt(1e6) + ns
  }
}

/**
 *
 * @param labels {Object}
 */
const stringify = (labels) => {
  labels = Object.fromEntries(Object.entries(labels).map(
    e => typeof e === 'string' ? [e[0], e[1]] : [e[0], `${e[1]}`]
  ))
  return stableStringify(labels)
}

/**
 *
 * @param attrs {*[]}
 * @returns {Object<string, string>}
 */
const flatOTLPAttrs = (attrs) => {
  const flatVal = (key, val, prefix, res) => {
    if (!val) {
      return
    }
    for (const valueKey of ['stringValue', 'boolValue', 'intValue', 'doubleValue', 'bytesValue']) {
      if (typeof val[valueKey] !== 'undefined') {
        res[prefix + key] = `${val[valueKey]}`
        return
      }
    }
    if (val.arrayValue) {
      val.arrayValue.values.forEach((v,i) => {
        flatVal(`${i}`, v, `${prefix}${key}.`, res);
      })
      return
    }
    if (val.kvlistValue) {
      flatAttrs(val.kvlistValue.values, `${prefix}${key}.`, res)
    }
  }
  const flatAttrs = (attrs, prefix, res) => {
    for (const attr of attrs) {
      if (!attr) {
        continue
      }
      flatVal(attr.key, attr.value, prefix, res)
    }
    return res
  }
  return flatAttrs(attrs, '', {})
}

let _samplesReadTableName = () => 'samples_read'
let _checkVersion = () => false

module.exports.DATABASE_NAME = () => process.env.CLICKHOUSE_DB || 'cloki'
module.exports.fingerPrint = fingerPrint
module.exports.labelParser = labelParser
module.exports.toJSON = toJSON
module.exports.parseMs = parseMs
module.exports.parseOrDefault = parseOrDefault

module.exports.onSamplesReadTableName = (fn) => { _samplesReadTableName = fn }
module.exports.onCheckVersion = (fn) => { _checkVersion = fn }

module.exports.samplesReadTableName = (from) => _samplesReadTableName(from)
module.exports.checkVersion = (ver, from) => _checkVersion(ver, from)

module.exports.schemaVer = 'v3'
module.exports.samplesTableName = 'samples_v3'
module.exports.parseStringifiedNanosOrRFC3339 = parseStringifiedNanosOrRFC3339
module.exports.parseDurationSecOrDefault = parseDurationSecOrDefault
module.exports.stringify = stringify
module.exports.flatOTLPAttrs = flatOTLPAttrs
