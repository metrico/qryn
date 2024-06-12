const { isMainThread, parentPort } = require('worker_threads')
const clickhouseOptions = require('./clickhouse_options').databaseOptions
const logger = require('../logger')
const { DATABASE_NAME } = require('../utils')
const clusterName = require('../../common').clusterName
const dist = clusterName ? '_dist' : ''
const { EventEmitter } = require('events')

// variables to be initialized in the init() function due to the './clickhouse.js' cross-dependency & bun
let samplesThrottler
let timeSeriesThrottler
let tracesThottler
let samplesTableName
let rawRequest

const axiosError = async (err) => {
  console.log('axiosError', err)
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
    console.log(e)
    return err
  }
}

class TimeoutOrSizeThrottler {
  constructor (statement, maxSizeB, maxAgeMS) {
    this.statement = statement
    this.queue = []
    this.resolvers = []
    this.rejects = []
    this.size = 0

    this.maxSizeB = maxSizeB
    this.maxAgeMs = maxAgeMS
    this.lastRequest = 0
  }

  /**
   * @param message {string}
   */
  queuePush (message) {
    this.queue.push(message)
    this.size += message.length
  }

  willFlush () {
    return (this.maxSizeB && this.size > this.maxSizeB) ||
      (this.maxAgeMs && Date.now() - this.lastRequest > this.maxAgeMs)
  }

  /**
   * @param force {boolean}
   * @returns {Promise<void>}
   */
  async flush (force) {
    try {
      if (!force && !this.willFlush()) {
        return
      }
      this.lastRequest = Date.now()
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
    await rawRequest(this.statement, _queue.join('\n'), DATABASE_NAME(), { maxBodyLength: Infinity })
  }

  stop () {
    this.on = false
  }
}

const emitter = new EventEmitter()
let on = true
const postMessage = message => {
  const genericRequest = (throttler) => {
    throttler.queuePush(message.data)
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
  [samplesTableName, rawRequest] = [
    require('./clickhouse').samplesTableName,
    require('./clickhouse').rawRequest
  ]

  samplesThrottler = new TimeoutOrSizeThrottler(
    `INSERT INTO ${clickhouseOptions.queryOptions.database}.${samplesTableName}${dist}(fingerprint, timestamp_ns, value, string, type) FORMAT JSONEachRow`,
    parseInt(process.env.BULK_MAX_SIZE_BYTES || 0), parseInt(process.env.BULK_MAX_AGE_MS || 100))
  timeSeriesThrottler = new TimeoutOrSizeThrottler(
    `INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series${dist}(date, fingerprint, labels, name, type) FORMAT JSONEachRow`,
    parseInt(process.env.BULK_MAX_SIZE_BYTES || 0), parseInt(process.env.BULK_MAX_AGE_MS || 100))
  tracesThottler = new TimeoutOrSizeThrottler(
    `INSERT INTO ${clickhouseOptions.queryOptions.database}.traces_input
      (trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, service_name, payload_type, payload, tags) 
    FORMAT JSONEachRow`,
    parseInt(process.env.BULK_MAX_SIZE_BYTES || 0), parseInt(process.env.BULK_MAX_AGE_MS || 100))

  setTimeout(async () => {
    // eslint-disable-next-line no-unmodified-loop-condition
    while (on) {
      try {
        await Promise.all([
          (async () => {
            await timeSeriesThrottler.flush(samplesThrottler.willFlush())
            await samplesThrottler.flush(false)
          })(),
          tracesThottler.flush(false)
        ])
      } catch (err) {
        logger.error(await axiosError(err), 'AXIOS ERROR')
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }, 0)
}

if (isMainThread) {
  module.exports = {
    samplesThrottler,
    timeSeriesThrottler,
    tracesThottler,
    TimeoutThrottler: TimeoutOrSizeThrottler,
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
