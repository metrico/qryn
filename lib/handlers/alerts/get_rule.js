const { CLokiNotFound } = require('../errors')
const clickhouse = require('../../db/clickhouse')

module.exports = async (req, res) => {
  const name = req.params.name
  const rule = await clickhouse.getAlertRule(name)
  if (!rule) {
    throw new CLokiNotFound(`Rule with name '${name}' not found`)
  }
  res.send(rule)
}
