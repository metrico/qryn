const Watcher = require('../db/watcher')
const logger = require('../logger')

module.exports = function handler (connection, res) {
  try {
    const w = new Watcher(res.query)
    w.on('data', s => {
      connection.socket.send(s)
    })
    w.on('error', err => {
      logger.error({ err })
      connection.socket.send(err)
      connection.end()
    })
    connection.socket.on('close', () => {
      w.removeAllListeners('data')
      w.destroy()
    })
  } catch (err) {
    logger.error({ err })
  }
}
