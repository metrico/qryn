/* Query Handler */
/*
  For doing queries, accepts the following parameters in the query-string:
  query: a logQL query
  limit: max number of entries to return
  start: the start time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
  end: the end time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
  direction: forward or backward, useful when specifying a limit
  regexp: a regex to filter the returned results, will eventually be rolled into the query language
*/

const { parseCliQL } = require('../cliql')
const { checkCustomPlugins } = require('./common')
const { asyncLogError, CORS } = require('../../common')
const { scanFingerprints, scanClickhouse } = require('../db/clickhouse')

async function handler (req, res) {
  req.log.debug('GET /loki/api/v1/query_range')
  const params = req.query
  const resp = { streams: [] }
  if (!req.query.query) {
    return res.send(resp)
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  if (!req.query.query) {
    return res.code(400).send('invalid query')
  }
  const cliqlParams = parseCliQL(req.query.query)
  if (cliqlParams) {
    scanClickhouse(cliqlParams, res, params)
    return
  }
  const pluginOut = await checkCustomPlugins(req.query)
  if (pluginOut) {
    res.header('Content-Type', pluginOut.type)
    return res.send(pluginOut.out)
  }
  req.query.optimizations = true
  try {
    const response = await scanFingerprints(req.query)
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
