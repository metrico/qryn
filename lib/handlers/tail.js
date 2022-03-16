const Watcher = require('../db/watcher')
const logger = require('../logger')

module.exports = function handler (connection, res) {
  try {
    const w = Watcher.getWatcher(res.query)
    const onData = (s) => {
      connection.socket.send(s)
    }
    const onError = err => {
      logger.error(err)
      connection.socket.send(err)
      connection.end()
    }
    w.on('data', onData)
    w.on('error', onError)
    connection.socket.on('close', () => {
      w.removeListener('data', onData)
      w.removeListener('error', onError)
      w.destroy()
    })
  } catch (err) {
    logger.error({ err })
  }
}
