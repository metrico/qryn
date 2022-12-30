const { ping, ready } = require('../db/clickhouse')
async function handler (req, res) {
  try {
    if (!ready()) {
      return res.send(new Error('qryn not ready'))
    }
    await ping()
    return res.code(204).send('OK')
  } catch (e) {
    return res.send(new Error('qryn DB not ready'))
  }
}
module.exports = handler
