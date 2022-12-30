/* Emulated PromQL Query Handler */

const { p2l } = require('@qxip/promql2logql');
const { asyncLogError, CORS } = require('../../common')
const empty = '{"status" : "success", "data" : {"resultType" : "scalar", "result" : []}}'; // to be removed
const test = () => `{"status" : "success", "data" : {"resultType" : "scalar", "result" : [${Math.floor(Date.now() / 1000)}, "2"]}}`; // to be removed
const exec = (val) => `{"status" : "success", "data" : {"resultType" : "scalar", "result" : [${Math.floor(Date.now() / 1000)}, val]}}`; // to be removed


async function handler (req, res) {
  req.log.debug('GET /loki/api/v1/query')
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
    return res.send(resp)
  }
  if (req.query.query === '1+1') {
    return res.status(200).send(test())
  }
  else if (!isNaN(parseInt(req.query.query))) {
    return res.status(200).send(exec(parseInt(req.query.query)))
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  /* transpile to logql */
  try {
    req.query.query = p2l(req.query.query)
  } catch(e) {
    asyncLogError({ e }, req.log)
    return res.send(empty)
  }
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    const response = await this.instantQueryScan(
      req.query
    )
    res.code(200)
    res.headers({
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': CORS
    })
    return response
  } catch (err) {
    asyncLogError(err, req.log)
    return res.send(empty)
  }
}

module.exports = handler
