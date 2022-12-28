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

async function handler (req, res) {
  req.log.debug('GET /api/search')
  const resp = { data: [] }
  if (!req.query.tags) {
    res.send(resp)
    return
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
    let resp = await this.scanTempo(
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
    res.send(resp)
  }
}

module.exports = handler
