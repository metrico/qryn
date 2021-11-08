const { ping, capabilities, ready } = require('../db/clickhouse')
async function handler (req, res) {
  try {
    if (!ready()) {
      res.send(new Error('cLoki not ready'))
      return
    }
    await ping()
    res.send({
      capabilities: {
        LIVE_mode: capabilities.LIVE_mode ? 'longpolling' : 'callback-polling'
      }
    })
  } catch (e) {
    res.send(new Error('Clickhouse DB not ready'))
  }
}
module.exports = handler
