const { getGroup } = require('../../db/alerting')
const yaml = require('yaml')
const { groupToResp, assertEnabled } = require('./common')

const testRule = (res) => {
  /** @type {alerting.group} */
  const group = {
    name: 'test',
    rules: [],
    interval: '1s'
  }
  res.header('Content-Type', 'yaml').send(yaml.stringify(group))
}

module.exports = (req, res) => {
  assertEnabled()
  if (req.params.ns === 'test' && req.params.group === 'test') {
    return testRule(res)
  }
  const grp = getGroup(req.params.ns, req.params.group)
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
