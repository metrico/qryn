const { dropGroup } = require('../../db/alerting')
module.exports = async (req, res) => {
  await dropGroup(req.params.ns, req.params.group)
  res.code(200).send('ok')
}
