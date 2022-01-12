const yaml = require('yaml')
const { setGroup } = require('../../db/alerting')
const { assertEnabled } = require('./common')
module.exports = async (req, res) => {
  assertEnabled()
  /** @type {alerting.group} */
  const group = req.body instanceof Object ? req.body : yaml.parse(req.body)
  await setGroup(req.params.ns, group)
  res.code(200).send({ msg: 'ok' })
}
