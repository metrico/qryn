/* Tempo Query Handler */

async function handler (req, res) {
  if (this.debug) console.log('GET /api/traces/:traceId')
  if (this.debug) console.log('QUERY: ', req.query)
   if (this.debug) console.log('TRACEID: ',req.params?.traceId)
  const resp = { traces: [] }
  if (!req.query.query || !req.params?.traceId) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = `{traceId: ${request.params.traceId}}`
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    /* TODO: Everything to make this look like a tempo traces reply */
    await this.instantQueryScan(
      req.query,
      { res: res.raw }
    )
  } catch (e) {
    console.log(e)
    res.send(resp)
  }
}

module.exports = handler
