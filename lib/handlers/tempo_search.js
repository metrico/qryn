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

async function handler (req, res) {
  req.log.debug('GET /api/search')
  const json_api = req.params.json || false;
  const resp = { data: [] }
  if (!req.params.tags) {
    res.send(resp)
    return
  }
	
  /* transpile trace params to logql selector */
  req.query.query = `{${req.query.tags}}`
  if (req.params.traceId) req.query.query += ` |~ "${req.params.traceId}"`
	
  req.log.debug('Search Tempo', req.query, req.query.tags);
	
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    const resp = await this.tempoSearchScan (
      req.query, res
    )
    let parsed = JSON.parse(resp);
    req.log.debug({ parsed }, 'PARSED');
    if(!parsed.data[0] || !parsed.data[0].spans) throw new Error('no results');
    
	  /* Send search results into JSON response */
	  req.log.debug({ struct }, 'PB-JSON');
    res.headers({'content-type': 'application/json'}).send(parsed)

  } catch (err) {
    req.log.error({ err })
    res.headers({'content-type': 'application/json'}).send(resp)
  }
}

module.exports = handler
