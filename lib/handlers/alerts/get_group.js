const { getGroup } = require('../../db/alerting')
const yaml = require('yaml')
const { groupToResp, assertEnabled } = require('./common')
const { AlertingClient } = require('../../db/clickhouse_alerting')

const testRule = (res) => {
  /** @type {alerting.group} */
  const group = {
    name: 'test',
    rules: [],
    interval: '1s'
  }
  res.header('Content-Type', 'yaml').send(yaml.stringify(group))
}

module.exports = async (req, res) => {
  /** @type {AlertingClient} */
  const client = new AlertingClient(await req.client())
  assertEnabled()
  if (req.params.ns === 'test' && req.params.group === 'test') {
    return testRule(res)
  }
  const grp = getGroup(req.params.ns, req.params.group, client)
  if (!grp) {
    /** @type {alerting.group} */
    const result = {
      name: req.params.group,
      interval: '1s',
      rules: []
    }
    res.header('Content-Type', 'yaml').send(yaml.stringify(result))
    return
  }
  const result = groupToResp({ ...grp })
  res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
