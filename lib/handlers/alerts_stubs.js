const { CLokiBadRequest, CLokiNotFound } = require('./errors')
const bnf = require('../../parser/bnf')
const alerts = {}

module.exports.createRule = (req, res) => {
  if (alerts[req.body.name]) {
    throw new CLokiBadRequest(`Rule with name '${req.body.name}' already exists`)
  }
  if (!bnf.ParseScript(req.body.request)) {
    throw new CLokiBadRequest(`Bad request '${req.body.request}'`)
  }
  alerts[req.body.name] = req.body
  res.send(req.body)
}

module.exports.getAlerts = (req, res) => {
  const limit = req.query.limit ? parseInt(req.query.limit) : 100
  const offset = req.query.limit ? parseInt(req.query.offset) : 0
  if (isNaN(limit)) {
    throw new CLokiBadRequest('limit is not a number')
  }
  if (isNaN(offset)) {
    throw new CLokiBadRequest('offset is not a number')
  }
  const result = Object.values(alerts)
  result.sort((a, b) => {
    return a.name.localeCompare(b.name)
  })
  res.send({
    alerts: result.length < offset ? [] : result.slice(offset, Math.min(result.length, offset + limit)),
    count: result.length
  })
}

module.exports.getAlert = (req, res) => {
  const name = req.params.name
  if (!alerts[name]) {
    throw new CLokiNotFound(`Rule with name '${name}' not found`)
  }
  res.send(alerts[name])
}

module.exports.putAlert = (req, res) => {
  const name = req.params.name
  if (!alerts[name]) {
    throw new CLokiNotFound(`Rule with name '${name}' not found`)
  }
  if (name !== req.body.name) {
    throw new CLokiBadRequest('Name can\'t be changed')
  }
  alerts[name] = req.body
  res.send(alerts[name])
}

module.exports.deleteAlert = (req, res) => {
  const name = req.params.name
  if (alerts[name]) {
    delete alerts[name]
  }
  res.send({ statusCode: 200, message: 'ok' })
}
