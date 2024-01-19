const { isMainThread, parentPort } = require('worker_threads')
const axios = require('axios')
const { getClickhouseUrl, samplesTableName } = require('./clickhouse_options')
const clickhouseOptions = require('./clickhouse_options').databaseOptions
const logger = require('../logger')
const clusterName = require('../../common').clusterName
const dist = clusterName ? '_dist' : ''
const { EventEmitter } = require('events')

const axiosError = async (err) => {
  try {
    const resp = err.response
    if (resp) {
      if (typeof resp.data === 'object') {
        err.responseData = ''
        err.response.data.on('data', data => { err.responseData += data })
        await new Promise((resolve) => err.response.data.once('end', resolve))
      }
      if (typeof resp.data === 'string') {
        err.responseData = resp.data
      }
      return new Error('AXIOS ERROR: ' + err +
        (err.responseData ? ' Response data: ' + err.responseData : ''))
    }
  } catch (e) {
    return err
  }
}

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
      logger.error(await axiosError(err), 'AXIOS ERROR')
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
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.${samplesTableName}${dist}(fingerprint, timestamp_ns, value, string) FORMAT JSONEachRow2`)
const timeSeriesThrottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series${dist}(date, fingerprint, labels, name) FORMAT JSONEachRow`)
const tracesThottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.traces_input
      (trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, service_name, payload_type, payload, tags) 
    FORMAT JSONEachRow`)

const emitter = new EventEmitter()
let on = true
const postMessage = message => {
  const genericRequest = (throttler) => {
    throttler.queue.push(message.data)
    throttler.resolvers.push(() => {
      if (isMainThread) {
        emitter.emit('message', { status: 'ok', id: message.id })
        return
      }
      parentPort.postMessage({ status: 'ok', id: message.id })
    })
    throttler.rejects.push(() => {
      if (isMainThread) {
        emitter.emit('message', { status: 'err', id: message.id })
        return
      }
      parentPort.postMessage({ status: 'err', id: message.id })
    })
  }
  switch (message.type) {
    case 'end':
      on = false
      if (!isMainThread) {
        parentPort.removeAllListeners('message')
      }
      break
    case 'values':
      genericRequest(samplesThrottler)
      break
    case 'labels':
      genericRequest(timeSeriesThrottler)
      break
    case 'traces':
      genericRequest(tracesThottler)
  }
}

const init = () => {
  setTimeout(async () => {
    // eslint-disable-next-line no-unmodified-loop-condition
    while (on) {
      const ts = Date.now()
      try {
        await timeSeriesThrottler.flush()
        await samplesThrottler.flush()
        await tracesThottler.flush()
      } catch (err) {
        logger.error(await axiosError(err), 'AXIOS ERROR')
      }
      const p = Date.now() - ts
      if (p < 100) {
        await new Promise((resolve) => setTimeout(resolve, 100 - p))
      }
    }
  }, 0)
}

if (isMainThread) {
  module.exports = {
    samplesThrottler,
    timeSeriesThrottler,
    tracesThottler,
    TimeoutThrottler,
    postMessage,
    on: emitter.on.bind(emitter),
    removeAllListeners: emitter.removeAllListeners.bind(emitter),
    init,
    terminate: () => {
      postMessage({ type: 'end' })
    }
  }
} else {
  init()
  parentPort.on('message', message => {
    postMessage(message)
  })
}
