const { dropNs } = require('../../db/alerting')
const { assertEnabled } = require('./common')
module.exports = async (req, res) => {
  assertEnabled()
  await dropNs(req.params.ns)
  res.code(200).send('ok')
}
