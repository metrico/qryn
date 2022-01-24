const Watcher = require('../db/watcher')

module.exports = async function handler (connection, res) {
  try {
    const client = await res.client()
    const w = new Watcher(res.query, client)
    w.on('data', s => {
      connection.socket.send(s)
    })
    w.on('error', err => {
      console.log(err)
      connection.socket.send(err)
      connection.end()
    })
    connection.socket.on('close', () => {
      w.removeAllListeners('data')
      w.destroy()
    })
  } catch (e) {
    console.log(e)
  }
}
