const yaml = require('yaml')
const { getAll } = require('../../db/alerting')
const { nsToResp } = require('./common')

module.exports = async (req, res) => {
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
  for (const k of Object.keys(result)) {
    result[k] = nsToResp(result[k])
  }
  console.log(yaml.stringify(result))
  res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
