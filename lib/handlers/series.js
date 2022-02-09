const { scanSeries } = require('../db/clickhouse')

// Example Handler
async function handler (req, res) {
  if (!req.query.match) {
    throw new Error('Match param is required')
  }
  await scanSeries(Array.isArray(req.query.match) ? req.query.match : [req.query.match],
    { res: res.raw })
  res.sent = true
}

module.exports = handler
