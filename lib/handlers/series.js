const { scanSeries } = require('../db/clickhouse')

// Example Handler
async function handler (req, res) {
  const getArray = (val) => {
    if (!val) {
      return []
    }
    return Array.isArray(val) ? val : [val]
  }
  let match = getArray(req.query.match)
  if (!match.length) {
    match = getArray(req.query['match[]'])
  }
  if (!match.length) {
    throw new Error('Match param is required')
  }
  await scanSeries(match, { res: res.raw })
  res.sent = true
}

module.exports = handler
