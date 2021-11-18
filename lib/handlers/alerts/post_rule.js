const { CLokiBadRequest } = require('../errors')
const bnf = require('../../../parser/bnf')
const clickhouse = require('../../db/clickhouse')

module.exports = async (req, res) => {
  if (!bnf.ParseScript(req.body.request)) {
    throw new CLokiBadRequest(`Bad request '${req.body.request}'`)
  }
  const rule = await clickhouse.getAlertRule(req.body.name)
  if (rule) {
    throw new CLokiBadRequest(`Rule with name '${req.body.name}' already exists`)
  }
  await clickhouse.putAlertRule(req.body.name, req.body.request, req.body.labels || {})
  res.send(req.body)
}
