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

async function handler (req, res) {
  if (this.debug) console.log('GET /loki/api/v1/query_range')
  if (this.debug) console.log('QUERY: ', req.query)
  const params = req.query
  const resp = { streams: [] }
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
  const cliqlParams = parseCliQL(req.query.query)
  if (cliqlParams) {
    this.scanClickhouse(cliqlParams, res, params)
  } else {
    try {
      await this.scanFingerprints(
        req.query,
        { res: res.raw }
      )
    } catch (e) {
      console.log(e)
      res.send(resp)
    }
  }
}

module.exports = handler
