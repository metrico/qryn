/* Emulated PromQL Query Handler */

async function buildinfo (req, res) {
  const path = req.url
  req.log.debug('PROM Handler', path)
  return res.send(
    {
      status: 'success',
      data: {
        version: '2.13.1',
        revision: 'cb7cbad5f9a2823a622aaa668833ca04f50a0ea7',
        branch: 'master',
        buildUser: 'qryn@qxip',
        buildDate: '29990401-13:37:420',
        goVersion: 'go1.18.1'
      }
    })
}

async function rules (req, res) {
  req.log.debug('PROM Handler', req.url)
  return res.send({
    status: 'success',
    data: {
      groups: [
        {
          rules: []
        }
      ]
    }
  })
}

async function misc (req, res) {
  res.send({ status: 'success', data: {} })
}

module.exports = {
  buildinfo,
  rules,
  misc
}
