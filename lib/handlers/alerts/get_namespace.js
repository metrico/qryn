const { getNs } = require('../../db/alerting')
const { nsToResp } = require('./common')
const yaml = require('yaml')
const { CLokiNotFound } = require('../errors')

module.exports = (req, res) => {
  const ns = getNs(req.params.ns)
  if (!ns) {
    throw CLokiNotFound('Namespace not found')
  }
  const result = nsToResp({ ...ns })
  console.log(yaml.stringify(result))
  res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
