const { getNs } = require('../../db/alerting')
const { nsToResp, assertEnabled } = require('./common')
const yaml = require('yaml')
const { QrynNotFound } = require('../errors')

module.exports = (req, res) => {
  assertEnabled()
  const ns = getNs(req.params.ns)
  if (!ns) {
    throw QrynNotFound('Namespace not found')
  }
  const result = nsToResp({ ...ns })
  return res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
