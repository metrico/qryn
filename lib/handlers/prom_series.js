const { scanSeries } = require('../db/clickhouse')
const { prom2labels, prom2log } = require('@qxip/promql2logql')
const { CORS } = require('../../common')

// Series Handler
async function handler (req, res) {
  const query = req.query.match || req.query['match[]']
  // bypass queries unhandled by transpiler
  if (query.includes('node_info')){
    res.send({ status: 'success', data: [] })
    return
  }
  // convert the input query into a label selector
  const labels = prom2labels(query)
  let match = getArray(labels)
  if (!match.length) {
    throw new Error('Match param is required')
  }
  const response = await scanSeries(match)
  response.code(200)
  response.headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': CORS
  })
  return response
}

const getArray = (val) => {
  if (!val) {
    return {}
  }
  return Array.isArray(val) ? val : [val]
}
module.exports = handler
