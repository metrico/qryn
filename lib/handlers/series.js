const { scanSeries } = require('../db/clickhouse')
const { CORS } = require('../../common')

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
  const response = await scanSeries(match)
  res.code(200)
  res.headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': CORS
  })
  return response
}

module.exports = handler
