const { isMainThread, parentPort } = require('worker_threads')
const axios = require('axios')
const { getClickhouseUrl, samplesTableName } = require('./clickhouse')
const clickhouseOptions = require('./clickhouse').databaseOptions
class TimeoutThrottler {
  constructor (statement) {
    this.statement = statement
    this.on = false
    this.queue = []
    this.resolvers = []
    this.rejects = []
  }

  start () {
    if (this.on) {
      return
    }
    this.on = true
    const self = this
    setTimeout(async () => {
      while (self.on) {
        const ts = Date.now()
        try {
          await self.flush()
          this.resolvers.forEach(r => r())
        } catch (e) {
          if (e.response) {
            console.log('AXIOS ERROR')
            console.log(e.message)
            console.log(e.response.status)
            console.log(e.response.data)
          } else {
            console.log(e)
          }
          self.rejects.forEach(r => r(e))
        }
        self.resolvers = []
        self.rejects = []
        const p = Date.now() - ts
        if (p < 100) {
          await new Promise((resolve) => setTimeout(resolve, 100 - p))
        }
      }
    })
  }

  async flush () {
    const len = this.queue.length
    if (len < 1) {
      return
    }
    const _queue = this.queue
    this.queue = []
    await axios.post(`${getClickhouseUrl()}/?query=${this.statement}`,
      _queue.join('\n'),
      {
        maxBodyLength: 200000000
      }
    )
  }

  stop () {
    this.on = false
  }
}

const samplesThrottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.${samplesTableName}(fingerprint, timestamp_ms, value, string) FORMAT JSONEachRow`)
const timeSeriesThrottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series(date, fingerprint, labels, name) FORMAT JSONEachRow`)

if (isMainThread) {
  module.exports = {
    samplesThrottler,
    timeSeriesThrottler,
    TimeoutThrottler
  }
} else {
  timeSeriesThrottler.start()
  samplesThrottler.start()
  parentPort.on('message', message => {
    switch (message.type) {
      case 'end':
        samplesThrottler.stop()
        timeSeriesThrottler.stop()
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
        timeSeriesThrottler.rejects.push((err) => {
          console.log(err)
          parentPort.postMessage({ status: 'err', id: message.id })
        })
    }
  })
}
