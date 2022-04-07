const yaml = require('yaml')
const { getAll } = require('../../db/alerting')
const { nsToResp, assertEnabled } = require('./common')

module.exports = async (req, res) => {
  assertEnabled()
  /** @type {Object<string, Object<string, alerting.objGroup>>} */
  const result = {
    fake: {
      fake: {
        name: 'fake',
        rules: {}
      }
    },
    ...getAll()
  }
  for (const ns of Object.values(result)) {
    for (const grp of Object.values(ns)) {
      for (const rul of Object.values(grp.rules)) {
        delete rul.ver
      }
    }
  }
  for (const k of Object.keys(result)) {
    result[k] = nsToResp(result[k])
  }
  res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
