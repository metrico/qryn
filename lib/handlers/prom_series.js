const { scanSeries } = require('../db/clickhouse')
const { prom2labels, prom2log } = require('@qxip/promql2logql')

// Series Handler
async function handler (req, res) {
  // convert the input query into a label selector
  const labels = prom2labels(req.query.match || req.query['match[]'])
  // console.log(req.query.match, labels)
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
