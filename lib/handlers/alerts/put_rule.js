const { CLokiNotFound, CLokiBadRequest } = require('../errors')
const bnf = require('../../../parser/bnf')
const { getAlertRule, putAlertRule } = require('../../db/clickhouse')
const { editAlert } = require('../../db/alerting')

module.exports.putAlert = async (req, res) => {
  if (!bnf.ParseScript(req.body.request)) {
    throw new CLokiBadRequest(`Bad request '${req.body.request}'`)
  }
  const name = req.params.name
  if (name !== req.body.name) {
    throw new CLokiBadRequest('Name can\'t be changed')
  }
  const rule = await getAlertRule(req.params.name)
  if (!rule) {
    throw new CLokiNotFound(`Rule with name '${name}' not found`)
  }
  await editAlert(req.body.name, req.body.request, req.body.labels)
  await putAlertRule(req.body.name, req.body.request, req.body.labels || {})
  res.send(req.body)
}
