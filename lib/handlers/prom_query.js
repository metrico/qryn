/* Emulated PromQL Query Handler */

const { p2l } = require('@qxip/promql2logql');

async function handler (req, res) {
  req.log.debug('GET /loki/api/v1/query')
  const resp = { streams: [] }
  if (!req.query.query) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  /* transpile to logql */
  try {
    req.query.query = p2l(req.query.query);
  } catch(e) {
    req.log.error({ err })
    res.send(resp)
  }
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    await this.instantQueryScan(
      req.query,
      { res: res.raw }
    )
  } catch (err) {
    req.log.error({ err })
    res.send(resp)
  }
}

module.exports = handler
