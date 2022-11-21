// Query Handler
async function handler (req, res) {
  req.log.debug('GET /loki/api/v1/query')
  const resp = { streams: [] }
  if (!req.query.query) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    await this.instantQueryScan(
      req.query,
      { res: res.raw }
    )
  } catch (err) {
    req.log.error({ err })
    throw err
  }
}

module.exports = handler
