const yaml = require('yaml')
const { setGroup } = require('../../db/alerting')
module.exports = async (req, res) => {
  /** @type {alerting.group} */
  console.log(req.body)
  const group = req.body instanceof Object ? req.body : yaml.parse(req.body)
  await setGroup(req.params.ns, group)
  res.code(200).send({ msg: 'ok' })
}
