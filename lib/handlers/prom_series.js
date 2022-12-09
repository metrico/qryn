const { scanSeries } = require('../db/clickhouse')
const { prom2labels, prom2log } = require('@qxip/promql2logql')

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
  await scanSeries(match, { res: res.raw })
  res.hijack()
}

const getArray = (val) => {
  if (!val) {
    return {}
  }
  return Array.isArray(val) ? val : [val]
}
module.exports = handler
