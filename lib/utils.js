/* Function Helpers */

/* Label Parser */
const labelParser = function (labels) {
  // Label Parser
  const rx = /\"?\b(\w+)\"?(!?=~?)("[^"\n]*?")/g
  let matches; const output = []
  while (matches = rx.exec(labels)) { // TODO: Comparison or assigment check
    if (matches.length > 3) output.push([matches[1], matches[2], matches[3].replace(/['"]+/g, '')])
  }
  try {
    var regex = /\}\s*(.*)/g.exec(labels)[1] || false
  } catch (e) {
    var regex = false
  }
  return { labels: output, regex: regex }
}
/* Fingerprinting */
const shortHash = require('short-hash')
const fingerPrint = function (text, hex) {
  if (hex) return shortHash(text)
  else return parseInt(shortHash(text), 16)
}

const toJSON = require('jsonic')

/* clickhouse query parser */
const clickParser = function (query) {
  /* Example cQL format */
  /* clickhouse({db="mydb", table="mytable", tag="key", metric="avg(value)", interval=60}) */
  const regx = /clickhouse\((.*)\)/g
  const clickQuery = regx.exec(req.query.query)[1] || false // TODO: req. correct?
  return labelParser(clickQuery)
}

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
module.exports.clickParser = clickParser
module.exports.toJSON = toJSON
module.exports.parseMs = parseMs
module.exports.parseOrDefault = parseOrDefault
