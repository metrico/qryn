const { ping, capabilities, ready } = require('../db/clickhouse')
async function handler (req, res) {
  try {
    if (!ready()) {
      return res.send(new Error('qryn not ready'))
    }
    await ping()
    return res.send({
      capabilities: {
        LIVE_mode: capabilities.LIVE_mode ? 'longpolling' : 'callback-polling'
      }
    })
  } catch (e) {
    return res.send(new Error('Clickhouse DB not ready'))
  }
}
module.exports = handler
