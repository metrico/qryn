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
const xxh = require('xxhashjs')
const xxhSeed = 'cloki'
const fingerPrint = function (text, hex) {
  if (hex) return xxh.h64(text, xxhSeed).toString(16)
  else return Number(xxh.h64(text, xxhSeed))
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

module.exports.DATABASE_NAME = () => process.env.CLICKHOUSE_DB || 'cloki'
module.exports.fingerPrint = fingerPrint
module.exports.labelParser = labelParser
module.exports.toJSON = toJSON
module.exports.parseMs = parseMs
module.exports.parseOrDefault = parseOrDefault
module.exports.samplesReadTableName = 'samples_read'
module.exports.samplesTableName = 'samples_v2'
