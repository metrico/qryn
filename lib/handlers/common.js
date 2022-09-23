const eng = require('../../plugins/engine')
const { parseCliQL } = require('../cliql')
const { Transform } = require('stream')
const { scanClickhouse, scanFingerprints } = require('../db/clickhouse')

module.exports.checkCustomPlugins = async (options) => {
  options.API = options.API || {
    logql: async (query, start, end, limit) => {
      const params = {
        query,
        start,
        end,
        limit,
        direction: 'backward',
        step: '60s'
      }
      const req = {
        query: params
      }
      const res = new Transform({
        transform (chunk, encoding, callback) {
          callback(null, chunk)
        }
      })
      res.writeHead = () => {}
      const cliqlParams = parseCliQL(req.query.query)
      if (cliqlParams) {
        scanClickhouse(cliqlParams, { res }, params)
      } else {
        await scanFingerprints(
          req.query,
          { res: res }
        )
      }
      let str = ''
      res.on('data', (d) => {
        str += d
      })
      await new Promise((resolve, reject) => {
        res.once('error', reject)
        res.once('close', resolve)
        res.once('end', resolve)
      })
      return JSON.parse(str)
    }/* ,
    promql: async () => {

    } */
  }
  const plugins = eng.getPlg({ type: 'custom_processor' })
  for (const plugin of Object.values(plugins)) {
    console.log(plugin)
    for (const e of Object.entries(options)) {
      plugin[e[0]] = e[1]
    }
    if (plugin.check()) {
      return await plugin.process()
    }
  }
}