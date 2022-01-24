const { dropNs } = require('../../db/alerting')
const { assertEnabled } = require('./common')
const { AlertingClient } = require('../../db/clickhouse_alerting')
module.exports = async (req, res) => {
  /** @type {CLokiClient} */
  const client = await req.client()
  assertEnabled()
  await dropNs(req.params.ns, new AlertingClient(client))
  res.code(200).send('ok')
}
