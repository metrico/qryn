/* Logging */
const pino = require('pino')

let level = process.env.LOG_LEVEL || 'info'

if (process.env.DEBUG && !process.env.LOG_LEVEL) {
  level = 'debug'
}

const logger = pino({
  name: 'cloki',
  level,
  serializers: {
    err: pino.stdSerializers.wrapErrorSerializer((err) => {
      if (err.response) {
        err.responseData = err.response.data
        err.responseStatus = err.response.status
        const res = new Error(`${err.message}\nResponse: [${err.response.status}] ${err.response.data}`)
        return res
      }
      return err
    })
  }
})

module.exports = logger
