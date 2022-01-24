// Query Handler
async function handler (req, res) {
  if (this.debug) console.log('GET /loki/api/v1/query')
  if (this.debug) console.log('QUERY: ', req.query)
  const resp = { streams: [] }
  if (!req.query.query) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  /** @type {CLokiClient} */
  const client = await req.client()
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    await client.instantQueryScan(
      req.query,
      { res: res.raw }
    )
  } catch (e) {
    console.log(e)
    res.send(resp)
  }
}

module.exports = handler
