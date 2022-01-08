const { dropGroup } = require('../../db/alerting')
const { assertEnabled } = require('./common')
module.exports = async (req, res) => {
  assertEnabled()
  await dropGroup(req.params.ns, req.params.group)
  res.code(200).send('ok')
}
