const yaml = require('yaml')
const { getAll } = require('../../db/alerting')
const { nsToResp, assertEnabled } = require('./common')
const { AlertingClient } = require('../../db/clickhouse_alerting')

module.exports = async (req, res) => {
  /** @type {AlertingClient} */
  const client = new AlertingClient(await req.client())
  assertEnabled()
  /** @type {Object<string, Object<string, alerting.objGroup>>} */
  const result = {
    fake: {
      fake: {
        name: 'fake',
        rules: {}
      }
    },
    ...getAll(client)
  }
  for (const k of Object.keys(result)) {
    result[k] = nsToResp(result[k])
  }
  res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
