const { transpileTail } = require('../../parser/transpiler')
const { networkInterfaces } = require('os')
const { getDatabaseIP, createMV } = require('../db/clickhouse')

/**
 *
 * @param ip {string}
 * @returns {BigInt}
 */
const ip2Int = (ip) => {
  if (ip.indexOf(':') !== -1) {
    return undefined
  }
  return ip.split('.').reduce((sum, oct) => (sum << BigInt(8)) + BigInt(parseInt(oct)), BigInt(0))
}

async function handler (req, res) {
  const q = transpileTail({
    query: req.query.query,
    suppressTime: true
  })
  const ip = ip2Int(await getDatabaseIP())
  if (!ip) {
    throw new Error(`ipv6 ${ip} not supported`)
  }
  const iface = Object.values(networkInterfaces())
    .reduce((sum, iface) => [...sum, ...iface], [])
    .filter(i => i.address.indexOf(':') === -1)
    .find(i => {
      const nm = ip2Int(i.netmask)
      return (ip & nm) === (ip2Int(i.address) & nm)
    })
  if (!iface) {
    throw new Error('Not reachable')
  }
  try {
    await createMV(q.query, 'mv_' + Math.random().toString().substr(2),
      `http://${iface.address}:${process.env.PORT || 3100}/alerts_data`
    )
  } catch (e) {
    console.log(e)
    throw e
  }
  res.send('ok')
}

module.exports = handler
