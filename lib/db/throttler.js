const { isMainThread, parentPort } = require('worker_threads')
const axios = require('axios')
const { getClickhouseUrl, samplesTableName } = require('./clickhouse')
const clickhouseOptions = require('./clickhouse').databaseOptions
const logger = require('../logger')
class TimeoutThrottler {
  constructor (statement) {
    this.statement = statement
    this.queue = []
    this.resolvers = []
    this.rejects = []
  }

  async flush () {
    try {
      await this._flush()
      this.resolvers.forEach(r => r())
    } catch (err) {
      logger.error(err, 'AXIOS ERROR')
      this.rejects.forEach(r => r(err))
    }
    this.resolvers = []
    this.rejects = []
  }

  async _flush () {
    const len = this.queue.length
    if (len < 1) {
      return
    }
    const _queue = this.queue
    this.queue = []
    await axios.post(`${getClickhouseUrl()}/?query=${this.statement}`,
      _queue.join('\n'),
      {
        maxBodyLength: Infinity
      }
    )
  }

  stop () {
    this.on = false
  }
}

const samplesThrottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.${samplesTableName}(fingerprint, timestamp_ns, value, string) FORMAT JSONEachRow`)
const timeSeriesThrottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series(date, fingerprint, labels, name) FORMAT JSONEachRow`)

if (isMainThread) {
  module.exports = {
    samplesThrottler,
    timeSeriesThrottler,
    TimeoutThrottler
  }
} else {
  let on = true
  setTimeout(async () => {
    // eslint-disable-next-line no-unmodified-loop-condition
    while (on) {
      const ts = Date.now()
      try {
        await timeSeriesThrottler.flush()
        await samplesThrottler.flush()
      } catch (err) {
        logger.error({ err }, 'AXIOS ERROR')
      }
      const p = Date.now() - ts
      if (p < 100) {
        await new Promise((resolve) => setTimeout(resolve, 100 - p))
      }
    }
  }, 0)
  parentPort.on('message', message => {
    switch (message.type) {
      case 'end':
        on = false
        parentPort.removeAllListeners('message')
        break
      case 'values':
        samplesThrottler.queue.push(message.data)
        samplesThrottler.resolvers.push(() => {
          parentPort.postMessage({ status: 'ok', id: message.id })
        })
        samplesThrottler.rejects.push(() => {
          parentPort.postMessage({ status: 'err', id: message.id })
        })
        break
      case 'labels':
        timeSeriesThrottler.queue.push(message.data)
        timeSeriesThrottler.resolvers.push(() => {
          parentPort.postMessage({ status: 'ok', id: message.id })
        })
        timeSeriesThrottler.rejects.push(() => {
          parentPort.postMessage({ status: 'err', id: message.id })
        })
    }
  })
}
