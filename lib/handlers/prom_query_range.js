/* Emulated PromQL Query Handler */
/*
  Converts PromQL to LogQL queries, accepts the following parameters in the query-string:
  query: a PromQL query
  limit: max number of entries to return
  start: the start time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
  end: the end time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
  direction: forward or backward, useful when specifying a limit
  regexp: a regex to filter the returned results, will eventually be rolled into the query language
*/

const { p2l } = require('@qxip/promql2logql')
const { asyncLogError, CORS } = require('../../common')

async function handler (req, res) {
  req.log.debug('GET /api/v1/query_range')
  const resp = {
    status: "success",
    data: {
      resultType: "vector",
      result: []
    }
  }
  if (req.method === 'POST') {
    req.query = req.body
  }
  if (!req.query.query) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  if (!req.query.query) {
    res.code(400).send('invalid query')
    return
  }
  // Convert PromQL to LogQL and execute
  try {
    req.query.query = p2l(req.query.query)
    const response = await this.scanFingerprints(
      {
        ...req.query,
        start: parseInt(req.query.start) * 1e9,
        end: parseInt(req.query.end) * 1e9
      }
    )
    res.code(200)
    res.headers({
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': CORS
    })
    return response
  } catch (err) {
    asyncLogError(err, req.log)
    res.send(resp)
  }
}

module.exports = handler
