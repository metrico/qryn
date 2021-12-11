const { dropNs } = require('../../db/alerting')
module.exports = async (req, res) => {
  await dropNs(req.params.ns)
  res.code(200).send('ok')
}
