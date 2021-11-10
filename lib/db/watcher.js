const transpiler = require('../../parser/transpiler')
const EventEmitter = require('events')
const UTILS = require('../utils')
const { queryFingerprintsScan, createLiveView, watchLiveView } = require('./clickhouse')
const { capabilities } = require('./clickhouse')

module.exports = class extends EventEmitter {
  constructor (request) {
    super()
    this.request = request

    this.step = UTILS.parseOrDefault(request.step, 5) * 1000
    const self = this
    this.working = true
    this.uid = Math.random()
    this.initQuery().catch(e => {
      if (self.working) {
        self.emit('error', e.message + '\n' + e.stack)
      }
    })
  }

  initQuery () {
    return capabilities.liveView ? this.initQueryWatchPoll() : this.initQueryCBPoll()
  }

  async initQueryWatchPoll () {
    this.watch = true
    const request = transpiler.transpileTail(this.request)
    const name = `watcher_${this.uid.toString().substr(2)}`
    await createLiveView(name, request.query, { timeout_sec: 10 })
    this.flushInterval = setInterval(this.flush.bind(this), 1000)
    while (this.working) {
      const [promise, cancel] = await watchLiveView(name, undefined,
        { res: this }, { stream: request.stream })
      this.cancel = cancel
      await promise
    }
  }

  async initQueryCBPoll () {
    this.from = (Date.now() - 300000) * 1000000
    while (this.working) {
      this.to = (Date.now() - 5000) * 1000000
      this.query = transpiler.transpile({
        ...this.request,
        start: this.from,
        end: this.to
      })
      this.query.step = this.step
      await queryFingerprintsScan(this.query, {
        res: this
      })
      this.from = this.to
      await new Promise((resolve, reject) => setTimeout(resolve, 1000))
    }
  }

  writeHead () {}
  isNewString (entry) {
    return !this.last || entry.timestamp_ms > this.last[0].timestamp_ms ||
            (entry.timestamp_ms === this.last[0].timestamp_ms &&
                !this.last.some(e => e.fingerprint === entry.fingerprint && e.string === entry.string))
  }

  write (str) {
    if (this.watch) {
      this.resp = this.resp || {}
      if (!this.isNewString(str)) {
        return
      }
      this.last = !this.last || str.timestamp_ms !== this.last[0].timestamp_ms ? [] : this.last
      this.last.push(str)
      const hash = JSON.stringify(str.labels)
      this.resp[hash] = this.resp[hash] || { stream: str.labels, values: [] }
      this.resp[hash].values.push([`${str.timestamp_ms}000000`, str.string])
      return
    }
    this.resp = this.resp || ''
    this.resp += str
  }

  flush () {
    if (!this.resp || Object.values(this.resp).length === 0) {
      return
    }
    this.emit('data', JSON.stringify({
      streams: Object.values(this.resp)
    }))
    this.resp = {}
  }

  end () {
    if (this.watch) {
      this.flush()
      return
    }
    this.emit('data', JSON.stringify(
      {
        streams: JSON.parse(this.resp).data.result
      }
    ))
    this.resp = ''
  }

  destroy () {
    this.working = false
    if (this.flushInterval) {
      clearInterval(this.flushInterval)
    }
    if (this.cancel) {
      this.cancel.cancel()
    }
  }
}
