/* Emulated PromQL Query Handler */

async function handler (req, res) {
  req.log.debug('GET /api/v1/*')
  const resp = {"status": "success", "data": {}};
  res.send(resp)
  return
}

module.exports = handler
