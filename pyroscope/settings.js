const messages = require('./settings_pb')
const services = require('./settings_grpc_pb')
const { parser, wrapResponse } = require('./shared')
const parsers = require('./json_parsers')

const get = (req, res) => {
  const _res = new messages.GetSettingsResponse()
  const s = new messages.Setting()
  s.setName('pluginSettings')
  s.setValue('{}')
  s.setModifiedat(Date.now())
  _res.setSettingsList([s])
  return _res
}

module.exports.init = (fastify) => {
  const fns = {
    get: get
  }
  const jsonParsers = {
    get: parsers.settingsGet
  }
  for (const name of Object.keys(fns)) {
    fastify.post(services.SettingsServiceService[name].path, (req, res) => {
      return wrapResponse(fns[name])(req, res)
    }, {
      'application/json': jsonParsers[name],
      '*': parser(services.SettingsServiceService[name].requestType)
    })
  }
}
