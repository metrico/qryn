const logger = require('./logger')
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
  alg = alg || process.env.HASH || 'short-hash'
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

const toJSON = require('jsonic')

const parseOrDefault = (str, def) => {
  try {
    return str ? parseFloat(str) : def
  } catch (e) {
    return def
  }
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

module.exports.DATABASE_NAME = () => process.env.CLICKHOUSE_DB || 'cloki'
module.exports.fingerPrint = fingerPrint
module.exports.labelParser = labelParser
module.exports.toJSON = toJSON
module.exports.parseMs = parseMs
module.exports.parseOrDefault = parseOrDefault
module.exports._samplesReadTableName = () => 'samples_read'
module.exports.samplesReadTableName = (from) => module.exports._samplesReadTableName(from)
module.exports.samplesTableName = 'samples_v3'
module.exports.parseStringifiedNanosOrRFC3339 = parseStringifiedNanosOrRFC3339
