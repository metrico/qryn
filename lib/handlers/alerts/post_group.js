const yaml = require('yaml')
const { setGroup } = require('../../db/alerting')
const { assertEnabled } = require('./common')
const { AlertingClient } = require('../../db/clickhouse_alerting')
module.exports = async (req, res) => {
  /** @type {AlertingClient} */
  const client = new AlertingClient(await req.client())
  assertEnabled()
  /** @type {alerting.group} */
  const group = req.body instanceof Object ? req.body : yaml.parse(req.body)
  await setGroup(req.params.ns, group, client)
  res.code(200).send({ msg: 'ok' })
}
