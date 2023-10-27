// Query Handler
const { asyncLogError, CORS } = require('../../common')
const { instantQueryScan } = require('../db/clickhouse')

async function handler (req, res) {
  req.log.debug('GET /loki/api/v1/query')
  const resp = { streams: [] }
  if (!req.query.query) {
    return res.send(resp)
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    const response = await instantQueryScan(req.query)
    res.code(200)
    res.headers({
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': CORS
    })
    return response
  } catch (err) {
    asyncLogError(err, req.log)
    throw err
  }
}

module.exports = handler
