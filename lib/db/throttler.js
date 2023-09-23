const { isMainThread, parentPort } = require('worker_threads')
const axios = require('axios')
const { getClickhouseUrl, samplesTableName } = require('./clickhouse')
const clickhouseOptions = require('./clickhouse').databaseOptions
const logger = require('../logger')

const axiosError = async (err) => {
  try {
    const resp = err.response
    if (resp) {
      if (typeof resp.data === 'object') {
        err.responseData = ''
        err.response.data.on('data', data => { err.responseData += data })
        await new Promise((resolve) => err.response.data.once('end', resolve))
      }
      return err
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
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.${samplesTableName}(fingerprint, timestamp_ns, value, string) FORMAT JSONEachRow`)
const timeSeriesThrottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series(date, fingerprint, labels, name) FORMAT JSONEachRow`)
const tracesThottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.traces_input
      (trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, service_name, payload_type, payload, tags) 
    FORMAT JSONEachRow`)
const profilesThottler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.pyroscope_profiles_input
      (timestamp_ns, type, sample_unit, sample_type, period_type, period_unit, tags, profile_id, duration_ns, value, payload_type, payload) 
    FORMAT JSONEachRow`)

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
        await tracesThottler.flush()
        await profilesThottler.flush()
      } catch (err) {
        logger.error(await axiosError(err), 'AXIOS ERROR')
      }
      const p = Date.now() - ts
      if (p < 100) {
        await new Promise((resolve) => setTimeout(resolve, 100 - p))
      }
    }
  }, 0)
  parentPort.on('message', message => {
    const genericRequest = (throttler) => {
      throttler.queue.push(message.data)
      throttler.resolvers.push(() => {
        parentPort.postMessage({ status: 'ok', id: message.id })
      })
      throttler.rejects.push(() => {
        parentPort.postMessage({ status: 'err', id: message.id })
      })
    }
    switch (message.type) {
      case 'end':
        on = false
        parentPort.removeAllListeners('message')
        break
      case 'values':
        genericRequest(samplesThrottler)
        break
      case 'labels':
        genericRequest(timeSeriesThrottler)
        break
      case 'traces':
        genericRequest(tracesThottler)
      case 'profiles':
        genericRequest(profilesThottler)
    }
  })
}
