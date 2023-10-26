/* Qryn Tempo Search Handler */
/*
   Returns JSON formatted results to /api/search API
  
   { "traces": [{ 
      "traceID":"AC62F5E32AFE5C28D4F8DCA4C159627E",
      "rootServiceName":"dummy-server",
      "rootTraceName":"request_response",
      "startTimeUnixNano":1661290946168377000,
      "durationMs":10
      }]
   }
   
*/

const logfmt = require('logfmt')
const common = require('../../common')
const { asyncLogError, CORS } = require('../../common')
const { scanTempo } = require('../db/clickhouse')
const { search } = require('../../traceql')

async function handler (req, res) {
  req.log.debug('GET /api/search')
  if (req.query.q) {
    return await searchV2(req, res)
  }
  const resp = { data: [] }
  if (!req.query.tags) {
    return res.send(resp)
  }
  /* transpile trace params to logql selector */
  let tags = logfmt.parse(req.query.tags)
  req.query.tags = tags
  req.log.debug(tags)
  tags = Object.entries(tags).map(e =>
    `${e[0].replace(/\../, m => `${m}`.toUpperCase().substring(1))}=${JSON.stringify(e[1])}`
  )
  req.query.start += '000000000'
  req.query.end += '000000000'
  req.query.query = `{${tags.join(', ')}}`
  if (req.params.traceId) req.query.query += ` |~ "${req.params.traceId}"`
  req.query.minDurationNs = req.query.minDuration ? common.durationToNs(req.query.minDuration) : undefined
  req.query.maxDurationNs = req.query.maxDuration ? common.durationToNs(req.query.maxDuration) : undefined

  req.log.debug(`Search Tempo ${req.query.query}, ${req.query.start}, ${req.query.end}`)
  try {
    let resp = await scanTempo(
      req.query
    )
    resp = [...resp.v2, ...resp.v1]
    res.code(200)
    res.headers({
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': CORS
    })
    return {
      traces: resp
    }
  } catch (err) {
    asyncLogError(err, req.log)
    return res.send(resp)
  }
}

const searchV2 = async (req, res) => {
  try {
    const query = req.query.q
    if (req.query.q === '{}') {
      return res.code(200).send(JSON.stringify({ traces: [] }))
    }
    const limit = req.query.limit || 100
    const start = req.query.start || Math.floor(Date.now() / 1000) - 3600
    const end = req.query.end || Math.floor(Date.now() / 1000) - 3600
    const traces = await search(query, limit, new Date(start * 1000), new Date(end * 1000))
    return res.code(200).send(JSON.stringify({ traces: traces }))
  } catch (e) {
    req.log.error(e)
    return res.code(500).send(e.message)
  }
}

module.exports = handler
