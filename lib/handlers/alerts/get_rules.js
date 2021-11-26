const { CLokiBadRequest } = require('../errors')
const clickhouse = require('../../db/clickhouse')

module.exports = async (req, res) => {
  const limit = req.query.limit ? parseInt(req.query.limit) : 100
  const offset = req.query.limit ? parseInt(req.query.offset) : 0
  const name = req.query.name
  if (isNaN(limit)) {
    throw new CLokiBadRequest('limit is not a number')
  }
  if (isNaN(offset)) {
    throw new CLokiBadRequest('offset is not a number')
  }
  const result = await clickhouse.getAlertRules(limit, offset, name)
  const count = await clickhouse.getAlertRulesCount()
  res.send({
    alerts: result,
    count: count
  })
}
