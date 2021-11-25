const { getAlertRule, deleteAlertRule } = require('../../db/clickhouse')
const { dropAlert } = require('../../db/alerting')

module.exports.deleteAlert = async (req, res) => {
  const name = req.params.name
  if (await getAlertRule(name)) {
    await dropAlert(name)
    await deleteAlertRule(req.params.name)
  }
  res.send({ statusCode: 200, message: 'ok' })
}
