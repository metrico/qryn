const { getNs } = require('../../db/alerting')
const { nsToResp, assertEnabled } = require('./common')
const yaml = require('yaml')
const { CLokiNotFound } = require('../errors')
const { AlertingClient } = require('../../db/clickhouse_alerting')

module.exports = async (req, res) => {
  /** @type {AlertingClient} */
  const client = new AlertingClient(await req.client())
  assertEnabled()
  const ns = getNs(req.params.ns, client)
  if (!ns) {
    throw CLokiNotFound('Namespace not found')
  }
  const result = nsToResp({ ...ns })
  res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
