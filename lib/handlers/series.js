// Example Handler
async function handler (req, res) {
  if (!req.query.match) {
    throw new Error('Match param is required')
  }
  /** @type {CLokiClient} */
  const client = await req.client()
  await client.scanSeries(Array.isArray(req.query.match) ? req.query.match : [req.query.match],
    { res: res.raw })
}

module.exports = handler
