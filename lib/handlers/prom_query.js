/* Emulated PromQL Query Handler */

const { p2l } = require('@qxip/promql2logql');
const { asyncLogError } = require('../../common')
const empty = '{"status" : "success", "data" : {"resultType" : "scalar", "result" : []}}'; // to be removed
const test = () => `{"status" : "success", "data" : {"resultType" : "scalar", "result" : [${Math.floor(Date.now() / 1000)}, "2"]}}`; // to be removed

async function handler (req, res) {
  req.log.debug('GET /loki/api/v1/query')
  const resp = { streams: [] }
  if (req.method === 'POST') {
    req.query = req.body
  }
  if (!req.query.query) {
    res.send(resp)
    return
  }
  if (req.query.query === '1+1') {
    return res.status(200).send(test())
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  /* transpile to logql */
  try {
    req.query.query = p2l(req.query.query);
  } catch(e) {
    asyncLogError({ e }, req.log)
    res.send(empty)
  }
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    await this.instantQueryScan(
      req.query,
      { res: res.raw }
    )
  } catch (err) {
    asyncLogError(err, req.log)
    res.send(empty)
  }
}

module.exports = handler
