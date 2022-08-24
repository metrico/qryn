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

const logger = require('../logger')
const logfmt = require('logfmt')

async function handler (req, res) {
  req.log.debug('GET /api/search')
  const json_api = req.params.json || false;
  const resp = { data: [] }
  if (!req.query.tags) {
    res.send(resp)
    return
  }
	
  /* transpile trace params to logql selector */
  let tags = logfmt.parse(req.query.tags)
  req.log.debug(tags)
  tags = Object.entries(tags).map(e =>
    `${e[0].replace(/\../, m => `${m}`.toUpperCase().substring(1))}=${JSON.stringify(e[1])}`
  )
  req.query.start += '000000000'
  req.query.end += '000000000'
  req.query.query = `{${tags.join(', ')}}`
  if (req.params.traceId) req.query.query += ` |~ "${req.params.traceId}"`
	
  req.log.debug(`Search Tempo ${req.query.query}, ${req.query.start}, ${req.query.end}`);
	
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    await this.scanTempo(
      req.query,
      { res: res.raw }
    )
    res.hijack()
  } catch (err) {
    req.log.error({ err })
    res.send(resp)
  }
}

module.exports = handler
