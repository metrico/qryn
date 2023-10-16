const { scanSeries } = require('../db/clickhouse')
const { CORS } = require('../../common')
const { isArray } = require('handlebars-helpers/lib/array')
const { QrynError } = require('./errors')
const {series} = require('../../promql/index')

// Series Handler
async function handler (req, res) {
  if (req.method === 'POST') {
    req.query = req.body
  }
  let query = req.query.match || req.query['match[]']
  // bypass queries unhandled by transpiler
  if (query.includes('node_info')) {
    return res.send({ status: 'success', data: [] })
  }
  if (!isArray(query)) {
    query = [query]
  }
  const startMs = req.query.start ? parseInt(req.query.start) * 1000 : Date.now() - 7 * 24 * 3600 * 1000
  const endMs = req.query.end ? parseInt(req.query.end) * 1000 : Date.now() - 7 * 24 * 3600 * 1000
  const result = []
  query = query.map(async (m) => {
    const _result = await series(m, startMs, endMs)
    result.push.apply(result, _result)
  })
  await Promise.all(query)
  return res.code(200).headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': CORS
  }).send(JSON.stringify({
    status: 'success',
    data: result
  }))
}

module.exports = handler
