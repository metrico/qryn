/* Logging */
const pino = require('pino')

let level = process.env.LOG_LEVEL || 'info'

if (process.env.DEBUG && !process.env.LOG_LEVEL) {
  level = 'debug'
}

const logger = pino({
  name: 'qryn',
  level,
  serializers: {
    err: pino.stdSerializers.wrapErrorSerializer((err) => {
      if (err.response) {
        err.responseData = err.response.data
        err.responseStatus = err.response.status
        const strData = typeof err.response.data === 'object' ? JSON.stringify(err.response.data) : err.response.data
        const res = new Error(`${err.message}\nResponse: [${err.response.status}] ${strData}`)
        res.stack = err.stack
        return res.toString() + '\n' + res.stack.toString()
      }
      return err.message.toString() + (err.stack ? '\n' + err.stack.toString() : '')
    })
  }
})

module.exports = logger
