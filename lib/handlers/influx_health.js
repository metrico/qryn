const { ping, ready } = require('../db/clickhouse')
async function handler (req, res) {
  try {
    if (!ready()) {
      res.send(new Error('qryn not ready'))
      return
    }
    await ping()
    res.code(204).send('OK')
  } catch (e) {
    res.send(new Error('qryn DB not ready'))
  }
}
module.exports = handler
