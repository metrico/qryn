/*
 * cLoki DB Adapter for Clickhouse
 * (C) 2018-2022 QXIP BV
 */

const debug = process.env.DEBUG || false
const UTILS = require('../utils')
const toJSON = UTILS.toJSON

const clickhouseOptions = require('./clickhouse_options').options()
const CORS = process.env.CORS_ALLOW_ORIGIN || '*'

const transpiler = require('../../parser/transpiler')
/* const rotationLabels = process.env.LABELS_DAYS || 7
const rotationSamples = process.env.SAMPLES_DAYS || 7 */
const axios = require('axios')
const { samplesTableName, samplesReadTableName } = UTILS

const getClickhouseUrl = require('./clickhouse_options').getUrl

// External Storage Policy for Tables (S3, MINIO)
// const storagePolicy = process.env.STORAGE_POLICY || false

const { StringStream, DataStream } = require('scramjet')

const { parseLabels, hashLabels } = require('../../common')

const capabilities = {}
const state = 'INITIALIZING'

let ch

class TimeoutThrottler {
  constructor (statement) {
    this.statement = statement
    this.on = false
    /**
     *
     * @type {Object<string, {client: CLokiClient, queue: string[]}>}
     */
    this.queue = {}
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
        } catch (e) {
          if (e.response) {
            console.log('AXIOS ERROR')
            console.log(e.message)
            console.log(e.response.status)
            console.log(e.response.data)
          } else {
            console.log(e)
          }
        }
        const p = Date.now() - ts
        if (p < 100) {
          await new Promise((resolve) => setTimeout(resolve, 100 - p))
        }
      }
    })
  }

  async flush () {
    for (const _queue of Object.entries(this.queue)) {
      const queue = _queue[1]
      const len = queue.queue.length
      if (len < 1) {
        continue
      }
      await queue.client.rawRequest(this.statement, queue.queue.join('\n'), null)
      queue.queue = queue.queue.slice(len)
    }
  }

  stop () {
    this.on = false
  }
}

const samplesThrottler = new TimeoutThrottler(
    `INSERT INTO ${samplesTableName}(fingerprint, timestamp_ms, value, string) FORMAT JSONEachRow`)
const timeSeriesThrottler = new TimeoutThrottler(
  'INSERT INTO time_series(date, fingerprint, labels, name) FORMAT JSONEachRow')
/* TODO: tsv2
const timeSeriesv2Throttler = new TimeoutThrottler(
  `INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series_v2(date, fingerprint, labels, name) FORMAT JSONEachRow`); */
samplesThrottler.start()
timeSeriesThrottler.start()
// timeSeriesv2Throttler.start();

/* Cache Helper */
const recordCache = require('record-cache')
const { parseMs } = require('../utils')
const { formatISO9075 } = require('date-fns')
const onStale = function (client) {
  return function (data) {
    for (const entry of data.records.entries()) {
      const value = entry[1]
      if (!samplesThrottler.queue[client.getClickhouseUrl()]) {
        samplesThrottler.queue[client.getClickhouseUrl()] = { client: client, queue: [] }
      }
      samplesThrottler.queue[client.getClickhouseUrl()].queue
        .push.apply(samplesThrottler.queue[client.getClickhouseUrl()].queue,
          value.list.map(r => JSON.stringify({
            fingerprint: r.record[0],
            timestamp_ms: r.record[1],
            value: r.record[2],
            string: r.record[3]
          })))
    }
  }
}
const onStaleLabels = function (client) {
  return function (data) {
    for (const entry of data.records.entries()) {
      const value = entry[1]
      if (!timeSeriesThrottler.queue[client.getClickhouseUrl()]) {
        timeSeriesThrottler.queue[client.getClickhouseUrl()] = { client: client, queue: [] }
      }
      timeSeriesThrottler.queue[client.getClickhouseUrl()].queue
        .push.apply(timeSeriesThrottler.queue[client.getClickhouseUrl()].queue,
          value.list.map(r => JSON.stringify({
            date: r.record[0],
            fingerprint: r.record[1],
            labels: r.record[2],
            name: r.record[3]
          })))
    }
  }
}

// Flushing to Clickhouse
const bulk = {
  putKey: function (idx, key, val) {
    if (!this._cache[idx]) {
      this._cache[idx] = recordCache({
        maxSize: process.env.BULK_MAXSIZE || 5000,
        maxAge: process.env.BULK_MAXAGE || 2000,
        onStale: onStale(new CLokiClient(idx))
      })
    }
    this._cache[idx].add(key, val)
  },
  getKey: function (idx, key) {
    if (!this._cache[idx]) {
      return undefined
    }
    return this._cache[idx].get(key)
  },
  _cache: {}
}

const bulkLabels = {
  putKey: function (idx, key, val) {
    if (!this._cache[idx]) {
      this._cache[idx] = recordCache({
        maxSize: 100,
        maxAge: 500,
        onStale: onStaleLabels(new CLokiClient(idx))
      })
    }
    this._cache[idx].add(key, val)
  },
  getKey: function (idx, key) {
    if (!this._cache[idx]) {
      return undefined
    }
    return this._cache[idx].get(key)
  },
  _cache: {}

}

// In-Memory LRU for quick lookups
const labels = {
  putKey: function (idx, key, val) {
    if (!this._cache[idx]) {
      this._cache[idx] = recordCache({
        maxSize: process.env.BULK_MAXCACHE || 50000,
        maxAge: 0,
        onStale: false
      })
    }
    this._cache[idx].add(key, val)
  },
  getKey: function (idx, key) {
    if (!this._cache[idx]) {
      return undefined
    }
    return this._cache[idx].get(key)
  },
  _cache: {}
}

const fakeStats = {
  summary: {
    bytesProcessedPerSecond: 0,
    linesProcessedPerSecond: 0,
    totalBytesProcessed: 0,
    totalLinesProcessed: 0,
    execTime: 0.001301608
  },
  store: {
    totalChunksRef: 0,
    totalChunksDownloaded: 0,
    chunksDownloadTime: 0,
    headChunkBytes: 0,
    headChunkLines: 0,
    decompressedBytes: 0,
    decompressedLines: 0,
    compressedBytes: 0,
    totalDuplicates: 0
  },
  ingester: {
    totalReached: 1,
    totalChunksMatched: 0,
    totalBatches: 0,
    totalLinesSent: 0,
    headChunkBytes: 0,
    headChunkLines: 0,
    decompressedBytes: 0,
    decompressedLines: 0,
    compressedBytes: 0,
    totalDuplicates: 0
  }
}

class CLokiClient {
  /**
   *
   * @param options {{} | string}
   */
  constructor (options) {
    let opts = clickhouseOptions
    if (!options) {
      this.url = new URL(`${opts.protocol}//${opts.auth}@${opts.host}:${opts.port}/?database=${opts.db}`)
    }
    if (options instanceof URL) {
      options = options.toString()
    }
    if (typeof options === 'string') {
      this.url = new URL(options)
      return
    }
    opts = {
      ...opts,
      ...options
    }
    this.url = new URL(`${opts.protocol}//${opts.auth}@${opts.host}:${opts.port}/?database=${opts.db}`)
  }
  /* Initialize */
  /* initialize (dbName) {
    console.log('Initializing DB...', dbName)
    const dbQuery = 'CREATE DATABASE IF NOT EXISTS ' + dbName
    const tmp = { ...clickhouseOptions, queryOptions: { database: '' } }
    ch = new ClickHouse(tmp)

    const hackCh = (ch) => {
      ch._query = ch.query
      ch.query = (q, opts, cb) => {
        return new Promise(resolve => ch._query(q, opts, (err, data) => {
          cb(err, data)
          resolve()
        }))
      }
    }
    return new Promise((resolve, reject) => {
      ch.query(dbQuery, undefined, async function (err) {
        if (err) {
          console.error('error', err)
          reject(err)
          return
        }
        const ch = new ClickHouse(clickhouseOptions)
        hackCh(ch)
        console.log('CREATE TABLES', dbName)

        let tsTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + '.time_series (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint'
        let smTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.${samplesTableName} (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (timestamp_ms)`
        const readTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.${samplesReadTableName} (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE=Merge(\'${dbName}\', \'(samples|samples_v[0-9]+)\')`
        const settingsTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + '.settings (fingerprint UInt64, type String, name String, value String, inserted_at DateTime64(9, \'UTC\')) ENGINE = ReplacingMergeTree(inserted_at) ORDER BY fingerprint'

        if (storagePolicy) {
          console.log('ADD SETTINGS storage policy', storagePolicy)
          const setStorage = ` SETTINGS storagePolicy='${storagePolicy}'`
          tsTable += setStorage
          smTable += setStorage
        }

        await ch.query(tsTable, undefined, function (err) {
          if (err) {
            console.log(err)
            process.exit(1)
          } else if (debug) console.log('Timeseries Table ready!')
          return true
        })
        await ch.query(smTable, undefined, function (err) {
          if (err) {
            console.log(err)
            process.exit(1)
          } else if (debug) console.log('Samples Table ready!')
          return true
        })
        await ch.query(readTable, undefined, function (err) {
          if (err) {
            console.log(err)
            process.exit(1)
          } else if (debug) console.log('Samples Table ready!')
          return true
        })
        await ch.query(settingsTable, undefined, function (err) {
          if (err) {
            console.log(err)
            process.exit(1)
          } else if (debug) console.log('Samples Table ready!')
          return true
        })

        if (rotationSamples > 0) {
          const alterTable = 'ALTER TABLE ' + dbName + `.${samplesTableName} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192`
          const rotateTable = 'ALTER TABLE ' + dbName + `.${samplesTableName} MODIFY TTL toDateTime(timestamp_ms / 1000)  + INTERVAL ` + rotationSamples + ' DAY'
          await ch.query(alterTable, undefined, function (err) {
            if (err) { console.log(err) } else if (debug) console.log('Samples Table altered for rotation!')
            // return true;
          })
          await ch.query(rotateTable, undefined, function (err) {
            if (err) { console.log(err) } else if (debug) console.log('Samples Table rotation set to days: ' + rotationSamples)
            return true
          })
        }

        if (rotationLabels > 0) {
          const alterTable = 'ALTER TABLE ' + dbName + '.time_series MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
          const rotateTable = 'ALTER TABLE ' + dbName + '.time_series MODIFY TTL date  + INTERVAL ' + rotationLabels + ' DAY'
          await ch.query(alterTable, undefined, function (err) {
            if (err) { console.log(err) } else if (debug) console.log('Labels Table altered for rotation!')
            return true
          })
          await ch.query(rotateTable, undefined, function (err) {
            if (err) { console.log(err) } else if (debug) console.log('Labels Table rotation set to days: ' + rotationLabels)
            return true
          })
        }

        if (storagePolicy) {
          console.log('ALTER storage policy', storagePolicy)
          const alterTs = `ALTER TABLE ${dbName}.time_series MODIFY SETTING storagePolicy='${storagePolicy}'`
          const alterSm = `ALTER TABLE ${dbName}.${samplesTableName} MODIFY SETTING storagePolicy='${storagePolicy}'`

          await ch.query(alterTs, undefined, function (err) {
            if (err) { console.log(err) } else if (debug) console.log('Storage policy update for fingerprints ' + storagePolicy)
            return true
          })
          await ch.query(alterSm, undefined, function (err) {
            if (err) { console.log(err) } else if (debug) console.log('Storage policy update for samples ' + storagePolicy)
            return true
          })
        }

        await checkCapabilities()

        state = 'READY'

        reloadFingerprints()
        resolve()
      })
    })
  } */

  /**
   * @returns {Promise<{liveView: boolean}>}
   */
  async checkCapabilities () {
    console.log('Checking clickhouse capabilities: ')
    if (this.capabilities) {
      return this.capabilities
    }
    try {
      await this.rawRequest(`CREATE LIVE VIEW ${clickhouseOptions.queryOptions.database}.lvcheck WITH TIMEOUT 1 AS SELECT 1`,
        null, { allow_experimental_live_view: '1' })
      this.capabilities = { liveView: true }
      console.log('LIVE VIEW: supported')
    } catch (e) {
      console.log('LIVE VIEW: unsupported')
      this.capabilities = { liveView: false }
    }
    return this.capabilities
  }

  async reloadFingerprints (force) {
    console.log('Reloading Fingerprints...')
    if (labels._cache[this.getClickhouseUrl()] && !force) {
      return
    }
    let rows = await this.rawRequest('SELECT DISTINCT fingerprint, labels FROM time_series FORMAT JSON',
      null, null)
    // or collect records yourself
    rows = rows.data && rows.data.data ? rows.data.data : []
    for (const row of rows) {
      const JSONLabels = toJSON(row.labels.replace(/\!?=/g, ':'))
      labels.putKey(this.getClickhouseUrl().toString(), row.fingerprint, JSON.stringify(JSONLabels))
      for (const key in JSONLabels) {
        labels.putKey(this.getClickhouseUrl().toString(), '_LABELS_', key)
        labels.putKey(this.getClickhouseUrl().toString(), key, JSONLabels[key])
      }
    }
  }

  /**
   *
   * @returns {Promise<string[]>}
   */
  async getLabels () {
    await this.reloadFingerprints()
    return labels.getKey(this.getClickhouseUrl().toString(), '_LABELS_')
  }

  /**
   *
   * @param name {string}
   * @returns {Promise<string[]>}
   */
  async getLabel (name) {
    return labels.getKey(this.getClickhouseUrl().toString(), name)
  }

  /**
   *
   * @param _labels {Object<string, string>}
   * @returns {Promise<number>}
   */
  async storeLabels (_labels) {
    const finger = UTILS.fingerPrint(JSON.stringify(_labels))
    labels.putKey(this.getClickhouseUrl().toString(), finger, JSON.stringify(_labels))
    bulkLabels.putKey(this.getClickhouseUrl().toString(), finger, [
      new Date().toISOString().split('T')[0],
      finger,
      JSON.stringify(_labels),
      _labels.name || ''
    ])
    return finger
  }

  /**
   *
   * @param fp {number}
   * @param log {[number, number, number, string]} [fingerprint, timestamp, value, log]
   * @returns {Promise<void>}
   */
  async storeLogs (fp, log) {
    bulk.putKey(this.getClickhouseUrl().toString(), fp, log)
  }

  /**
   *
   * @param label {string}
   * @returns {Promise<string[]>}
   */
  async getLabelValues (label) {
    await this.reloadFingerprints()
    return labels.getKey(this.getClickhouseUrl().toString(), label)
  }

  async scanFingerprints (query, res) {
    if (debug) console.log('Scanning Fingerprints...')
    const _query = transpiler.transpile(query)
    _query.step = UTILS.parseOrDefault(query.step, 5) * 1000
    return this.queryFingerprintsScan(_query, res)
  }

  async instantQueryScan (query, res) {
    if (debug) console.log('Scanning Fingerprints...')
    const time = parseMs(query.time, Date.now())
    query.start = (time - 10 * 60 * 1000) * 1000000
    query.end = Date.now() * 1000000
    const _query = transpiler.transpile(query)
    _query.step = UTILS.parseOrDefault(query.step, 5) * 1000

    const _stream = await this.getClickhouseStream(_query.query)
    const dataStream = this.preprocessStream(_stream, _query.stream || [])
    return await (_query.matrix
      ? this.outputQueryVector(dataStream, res)
      : this.outputQueryStreams(dataStream, res))
  }

  async tempoQueryScan (query, res, traceId) {
    if (debug) console.log('Scanning Tempo Fingerprints...', traceId)
    const time = parseMs(query.time, Date.now())
    /* Tempo does not seem to pass start/stop parameters. Use ENV or default 24h */
    const hours = this.tempo_span || 24
    if (!query.start) query.start = (time - (hours * 60 * 60 * 1000)) * 1000000
    if (!query.end) query.end = Date.now() * 1000000
    const _query = transpiler.transpile(query)
    _query.step = UTILS.parseOrDefault(query.step, 5) * 1000

    const _stream = await this.getClickhouseStream(_query.query)
    const dataStream = this.preprocessStream(_stream, _query.stream || [])
    console.log('debug tempo', query)
    return await (this.outputTempoSpans(dataStream, res, traceId))
  }

  /**
   * @param query {
   * {query: string, duration: number, matrix: boolean, stream: (function(DataStream): DataStream)[], step: number}
   * }
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   * @returns {Promise<void>}
   */
  async queryFingerprintsScan (query, res) {
    if (debug) console.log('Scanning Fingerprints...')
    // console.log(_query.query);
    const _stream = await this.getClickhouseStream(query.query)
    const dataStream = this.preprocessStream(_stream, query.stream || [])
    return await (query.matrix
      ? this.outputQueryMatrix(dataStream, res, query.step, query.duration)
      : this.outputQueryStreams(dataStream, res))
  }

  /**
   *
   * @param query {string}
   * @param format {string}
   * @returns {Promise<Stream>}
   */
  getClickhouseStream (query, format) {
    format = format || 'JSONEachRow'
    return axios.post(this.getClickhouseUrl().toString(),
      query + ' FORMAT ' + format,
      {
        responseType: 'stream'
      }
    )
  }

  /**
   *
   * @param dataStream {DataStream}
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   * @returns {Promise<void>}
   */
  async outputQueryStreams (dataStream, res) {
    res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
    const gen = dataStream.toGenerator()
    let i = 0
    let lastLabels = null
    let lastStream = []
    res.res.write('{"status":"success", "data":{ "resultType": "streams", "result": [')
    for await (const item of gen()) {
      if (!item) {
        continue
      }
      if (!item.labels) {
        if (!lastLabels || !lastStream.length) {
          continue
        }
        res.res.write(i ? ',' : '')
        res.res.write(JSON.stringify({
          stream: parseLabels(lastLabels),
          values: lastStream
        }))
        lastLabels = null
        lastStream = []
        ++i
        continue
      }
      const hash = hashLabels(item.labels)
      const ts = item.timestamp_ms ? parseInt(item.timestamp_ms) : null
      if (hash === lastLabels) {
        ts && lastStream.push([(ts * 1000000).toString(), item.string])
        continue
      }
      if (lastLabels) {
        res.res.write(i ? ',' : '')
        res.res.write(JSON.stringify({
          stream: parseLabels(lastLabels),
          values: lastStream
        }))
        ++i
      }
      lastLabels = hash
      lastStream = ts ? [[(ts * 1000000).toString(), item.string]] : []
    }
    res.res.write(']}}')
    res.res.end()
  }

  /**
   *
   * @param dataStream {DataStream}
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   * @param stepMs {number}
   * @param durationMs {number}
   * @returns {Promise<void>}
   */
  async outputQueryMatrix (dataStream, res,
    stepMs, durationMs) {
    res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
    const addPoints = Math.ceil(durationMs / stepMs)
    const gen = dataStream.toGenerator()
    let i = 0
    let lastLabels = null
    let lastStream = []
    let lastTsMs = 0
    res.res.write('{"status":"success", "data":{ "resultType": "matrix", "result": [')
    for await (const item of gen()) {
      if (!item) {
        continue
      }
      if (!item.labels) {
        if (!lastLabels || !lastStream.length) {
          continue
        }
        res.res.write(i ? ',' : '')
        res.res.write(JSON.stringify({
          metric: parseLabels(lastLabels),
          values: lastStream
        }))
        lastLabels = null
        lastStream = []
        lastTsMs = 0
        ++i
        continue
      }
      const hash = hashLabels(item.labels)
      const ts = item.timestamp_ms ? parseInt(item.timestamp_ms) : null
      if (hash === lastLabels) {
        if (ts < (lastTsMs + stepMs)) {
          continue
        }
        for (let j = 0; j < addPoints; ++j) {
          ts && lastStream.push([(ts + stepMs * j) / 1000, item.value.toString()])
        }
        lastTsMs = ts
        continue
      }
      if (lastLabels) {
        res.res.write(i ? ',' : '')
        res.res.write(JSON.stringify({
          metric: parseLabels(lastLabels),
          values: lastStream
        }))
        ++i
      }
      lastLabels = hash
      lastStream = []
      for (let j = 0; j < addPoints; ++j) {
        ts && lastStream.push([(ts + stepMs * j) / 1000, item.value.toString()])
      }
      lastTsMs = ts
    }
    res.res.write(']}}')
    res.res.end()
  }

  /**
   *
   * @param dataStream {DataStream}
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   * @returns {Promise<void>}
   */
  async outputQueryVector (dataStream, res) {
    res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
    const gen = dataStream.toGenerator()
    let i = 0
    let lastLabels = null
    let lastTsMs = 0
    let lastValue = 0
    res.res.write('{"status":"success", "data":{ "resultType": "vector", "result": [')
    for await (const item of gen()) {
      if (!item) {
        continue
      }
      if (!item.labels) {
        if (!lastLabels || !lastTsMs) {
          continue
        }
        res.res.write(i ? ',' : '')
        res.res.write(JSON.stringify({
          metric: parseLabels(lastLabels),
          value: [lastTsMs / 1000, lastValue.toString()]
        }))
        lastLabels = null
        lastTsMs = 0
        ++i
        continue
      }
      const hash = hashLabels(item.labels)
      const ts = item.timestamp_ms ? parseInt(item.timestamp_ms) : null
      if (hash === lastLabels) {
        lastTsMs = ts
        lastValue = item.value
        continue
      }
      if (lastLabels) {
        res.res.write(i ? ',' : '')
        res.res.write(JSON.stringify({
          metric: parseLabels(lastLabels),
          value: [lastTsMs / 1000, lastValue.toString()]
        }))
        ++i
      }
      lastLabels = hash
      lastTsMs = ts
      lastValue = item.value
    }
    res.res.write(']}}')
    res.res.end()
  }

  /**
   *
   * @param dataStream {DataStream}
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   * @param traceId {String}
   * @returns {Promise<any>}
   */
  async outputTempoSpans (dataStream, res, traceId) {
    // res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
    const gen = dataStream.toGenerator()
    let i = 0
    let lastLabels = null
    let lastStream = []
    let response = '{"total": 0, "limit": 0, "offset": 0, "errors": null, "processes" : { "p1": {} }, "data": [ { "traceID": "' + traceId + '", '
    response += '"spans":['
    for await (const item of gen()) {
      if (!item) {
        continue
      }
      if (!item.labels) {
        if (!lastLabels || !lastStream.length) {
          continue
        }
        response += (i ? ',' : '')
        response += JSON.stringify(lastStream[0])
        /*
        res.res.write(JSON.stringify({
          traceID: lastLabels.traceId,
          spans: lastStream
        }))
        */
        lastLabels = null
        lastStream = []
        ++i
        continue
      }
      const hash = hashLabels(item.labels)
      const ts = item.timestamp_ms ? parseInt(item.timestamp_ms) : null
      if (hash === lastLabels) {
        ts && lastStream.push(JSON.parse(item.string))
        continue
      }
      if (lastLabels) {
        response += (i ? ',' : '')
        response += (JSON.stringify(lastStream[0]))
        /*
        res.res.write(JSON.stringify({
          traceID: lastLabels.traceId,
          spans: lastStream
        }))
        */
        ++i
      }
      lastLabels = hash
      lastStream = ts ? [JSON.parse(item.string)] : []
    }
    response += (']}]}')
    return response
  }

  /**
   *
   * @param rawStream {any} Stream from axios response
   * @param processors {(function(DataStream): DataStream)[] | undefined}
   * @returns {DataStream}
   */
  preprocessStream (rawStream, processors) {
    let dStream = StringStream.from(rawStream.data).lines().endWith(JSON.stringify({ EOF: true }))
      .map(chunk => chunk ? JSON.parse(chunk) : ({}), DataStream)
      .map(chunk => {
        try {
          if (!chunk || !chunk.labels) {
            return chunk
          }
          const labels = chunk.extra_labels
            ? { ...parseLabels(chunk.labels), ...parseLabels(chunk.extra_labels) }
            : parseLabels(chunk.labels)
          return { ...chunk, labels: labels }
        } catch (e) {
          console.log(chunk)
          return chunk
        }
      }, DataStream)
    if (processors && processors.length) {
      processors.forEach(f => {
        dStream = f(dStream)
      })
    }
    return dStream
  }

  /* cLoki Metrics Column */
  scanMetricFingerprints (settings, client, params) {
    if (debug) console.log('Scanning Clickhouse...', settings)
    // populate matrix structure
    const resp = {
      status: 'success',
      data: {
        resultType: 'matrix',
        result: []
      }
    }
    // Check for required fields or return nothing!
    if (!settings || !settings.table || !settings.db || !settings.tag || !settings.metric) {
      client.send(resp)
      return
    }
    settings.interval = settings.interval ? parseInt(settings.interval) : 60
    if (!settings.timefield) settings.timefield = process.env.CLICKHOUSE_TIMEFIELD || 'record_datetime'

    const tags = settings.tag.split(',')
    let template = 'SELECT ' + tags.join(', ') + ', groupArray((toUnixTimestamp(timestamp_ms)*1000, toString(value))) AS groupArr FROM (SELECT '
    if (tags) {
      tags.forEach(function (tag) {
        tag = tag.trim()
        template += " visitParamExtractString(labels, '" + tag + "') as " + tag + ','
      })
    }
    // if(settings.interval > 0){
    template += ' toStartOfInterval(toDateTime(timestamp_ms/1000), INTERVAL ' + settings.interval + ' second) as timestamp_ms, value' +
      ' FROM ' + settings.db + '.samples RIGHT JOIN ' + settings.db + '.time_series ON samples.fingerprint = time_series.fingerprint'
    if (params.start && params.end) {
      template += ' WHERE ' + settings.timefield + ' BETWEEN ' + parseInt(params.start / 1000000000) + ' AND ' + parseInt(params.end / 1000000000)
      // template += " WHERE "+settings.timefield+" BETWEEN "+parseInt(params.start/1000000) +" AND "+parseInt(params.end/1000000)
    }
    if (tags) {
      tags.forEach(function (tag) {
        tag = tag.trim()
        template += " AND (visitParamExtractString(labels, '" + tag + "') != '')"
      })
    }
    if (settings.where) {
      template += ' AND ' + settings.where
    }
    template += ' AND value > 0 ORDER BY timestamp_ms) GROUP BY ' + tags.join(', ')

    if (debug) console.log('CLICKHOUSE METRICS SEARCH QUERY', template)

    const stream = ch.query(template)
    // or collect records yourself
    const rows = []
    stream.on('metadata', function (columns) {
      // do something with column list
    })
    stream.on('data', function (row) {
      rows.push(row)
    })
    stream.on('error', function (err) {
      // TODO: handler error
      client.code(400).send(err)
    })
    stream.on('end', function () {
      if (debug) console.log('CLICKHOUSE RESPONSE:', rows)
      if (!rows || rows.length < 1) {
        resp.data.result = []
        resp.data.stats = fakeStats
      } else {
        try {
          rows.forEach(function (row) {
            const metrics = { metric: {}, values: [] }
            const tags = settings.tag.split(',')
            // bypass empty blocks
            if (row[row.length - 1].length < 1) return
            // iterate tags
            for (let i = 0; i < row.length - 1; i++) {
              metrics.metric[tags[i]] = row[i]
            }
            // iterate values
            row[row.length - 1].forEach(function (row) {
              if (row[1] === 0) return
              metrics.values.push([parseInt(row[0] / 1000), row[1].toString()])
            })
            resp.data.result.push(metrics)
          })
        } catch (e) { console.log(e) }
      }
      if (debug) console.log('CLOKI RESPONSE', JSON.stringify(resp))
      client.send(resp)
    })
  }

  /**
   * Clickhouse Metrics Column Query
   * @param settings {{
   *   db: string,
   *   table: string,
   *   interval: string | number,
   *   tag: string,
   *   metric: string
   * }}
   * @param client {{
   *   code: function(number): any,
   *   send: function(string): any
   * }}
   * @param params {{
   *   start: string | number,
   *   end: string | number,
   *   shift: number | undefined
   * }}
   */
  async scanClickhouse (settings, client, params) {
    if (debug) console.log('Scanning Clickhouse...', settings)

    // populate matrix structure
    const resp = {
      status: 'success',
      data: {
        resultType: 'matrix',
        result: []
      }
    }

    // TODO: Replace this template with a proper parser!
    // Check for required fields or return nothing!
    if (!settings || !settings.table || !settings.db || !settings.tag || !settings.metric) {
      client.send(resp)
      return
    }
    settings.interval = settings.interval ? parseInt(settings.interval) : 60
    // Normalize timefield
    if (!settings.timefield) settings.timefield = process.env.TIMEFIELD || 'record_datetime'
    else if (settings.timefield === 'false') settings.timefield = false
    // Normalize Tags
    if (settings.tag.includes('|')) { settings.tag = settings.tag.split('|').join(',') }
    // Lets query!
    let template = 'SELECT ' + settings.tag + ', groupArray((t, c)) AS groupArr FROM ('
    // Check for timefield or Bypass timefield
    if (settings.timefield) {
      const shiftSec = params.shift ? params.shift / 1000 : 0
      const timeReq = params.shift
        ? `intDiv(toUInt32(${settings.timefield} - ${shiftSec}), ${settings.interval}) * ${settings.interval} + ${shiftSec}`
        : 'intDiv(toUInt32(' + settings.timefield + '), ' + settings.interval + ') * ' + settings.interval
      template += `SELECT (${timeReq}) * 1000 AS t, ` + settings.tag + ', ' + settings.metric + ' c '
    } else {
      template += 'SELECT toUnixTimestamp(now()) * 1000 AS t, ' + settings.tag + ', ' + settings.metric + ' c '
    }
    template += 'FROM ' + settings.db + '.' + settings.table
    // Check for timefield or standalone where conditions
    if (params.start && params.end && settings.timefield) {
      template += ' PREWHERE ' + settings.timefield + ' BETWEEN ' + parseInt(params.start / 1000000000) + ' AND ' + parseInt(params.end / 1000000000)
      if (settings.where) {
        template += ' AND ' + settings.where
      }
    } else if (settings.where) {
      template += ' WHERE ' + settings.where
    }
    template += ' GROUP BY t, ' + settings.tag + ' ORDER BY t, ' + settings.tag + ')'
    template += ' GROUP BY ' + settings.tag + ' ORDER BY ' + settings.tag
    if (debug) console.log('CLICKHOUSE SEARCH QUERY', template)

    const stream = await this.getClickhouseStream(template, 'JSONCompactEachRow') // ch.query(template)
    const rows = []
    StringStream.from(stream.data).lines().map(row => {
      if (!row) {
        return null
      }
      try {
        return JSON.parse(row)
      } catch (e) {
        console.log(e)
        return null
      }
    }, DataStream).endWith({ _EOF: true }).each((oRow) => {
      if (!oRow.EOF) {
        rows.push(oRow)
        return
      }
      if (debug) console.log('CLICKHOUSE RESPONSE:', rows)
      if (!rows || rows.length < 1) {
        resp.data.result = []
        resp.data.stats = fakeStats
      } else {
        try {
          rows.forEach(function (row) {
            const metrics = { metric: {}, values: [] }
            const tags = settings.tag.split(',').map(t => t.trim())
            // bypass empty blocks
            if (row[row.length - 1].length < 1) return
            // iterate tags
            for (let i = 0; i < row.length - 1; i++) {
              metrics.metric[tags[i]] = row[i]
            }
            // iterate values
            row[row.length - 1].forEach(function (row) {
              if (row[1] === 0) return
              metrics.values.push([parseInt(row[0] / 1000), row[1].toString()])
            })
            resp.data.result.push(metrics)
          })
        } catch (e) { console.log(e) }
      }
      if (debug) console.log('CLOKI RESPONSE', JSON.stringify(resp))
      client.send(resp)
    })
  }

  /**
   *
   * @param matches {string[]} ['{ts1="a1"}', '{ts2="a2"}', ...]
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   */
  async getSeries (matches, res) {
    const query = transpiler.transpileSeries(matches)
    const stream = await this.getClickhouseStream(query)
    const dStream = StringStream.from(stream.data).lines().map(l => {
      if (!l) {
        return null
      }
      try {
        return JSON.parse(l)
      } catch (e) {
        console.log(l)
        console.log(e)
        return null
      }
    }, DataStream).filter(e => e)
    const gen = dStream.toGenerator()
    res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
    res.res.write('{"status":"success", "data":[')
    let i = 0
    for await (const item of gen()) {
      if (!item || !item.labels) {
        continue
      }
      res.res.write((i === 0 ? '' : ',') + item.labels)
      ++i
    }
    res.res.write(']}')
    res.res.end()
  }

  async ping () {
    await this.rawRequest('SELECT 1', null, null)
  }

  /* Module Exports */

  /**
   *
   * @param name {string}
   * @param request {string}
   * @param options {{db : string | undefined, timeout_sec: number | undefined}}
   */
  createLiveView (name, request, options) {
    const timeout = options.timeout_sec ? `WITH TIMEOUT ${options.timeout_sec}` : ''
    return this.rawRequest(`CREATE LIVE VIEW ${name} ${timeout} AS ${request}`, null, {
      allow_experimental_live_view: '1'
    })
  }

  /**
   *
   * @param db {string}
   * @param name {string}
   * @param name {string}
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   * @param options {{
   *     stream: (function(DataStream): DataStream)[],
   * }}
   * @returns Promise<[Promise<void>, CancelTokenSource]>
   */
  async watchLiveView (name, db, res, options) {
    const cancel = axios.CancelToken.source()
    const url = this.getClickhouseUrl()
    url.searchParams.append('allow_experimental_live_view', '1')
    const stream = await axios.post(url.toString(),
      `WATCH ${db}.${name} FORMAT JSONEachRow`,
      {
        responseType: 'stream',
        cancelToken: cancel.token
      })
    const endPromise = (async () => {
      const _stream = this.preprocessStream(stream, options.stream)
      const gen = _stream.toGenerator()
      res.res.writeHead(200, {})
      for await (const item of gen()) {
        if (!item || !item.labels) {
          continue
        }
        res.res.write(item)
      }
      res.res.end()
    })()
    return [endPromise, cancel]
  }

  async createMV (query, id, url) {
    const request = `CREATE MATERIALIZED VIEW ${clickhouseOptions.queryOptions.database}.${id} ` +
      `ENGINE = URL('${url}', JSON) AS ${query}`
    console.log('MV: ' + request)
    await this.rawRequest(request, null, null)
  }

  /**
   * @returns {Promise<{url: string, samples_days: number,
   *   time_series_days: number, storage_policy: string}[]>}
   */
  async getAllDBs () {
    return Object.values(await this.getAllOrgs())
  }

  /**
   * @returns {Promise<Object<string, {url: string, samples_days: number,
   *   time_series_days: number, storage_policy: string}>>}
   */
  async getAllOrgs () {
    return {}
  }

  /**
   *
   * @param names {{type: string, name: string}[]}
   * @returns {Promise<Object<string, string>>}
   */
  async getSettings (names) {
    const fps = names.map(n => this.getFP(n.type, n.name))
    const rows = await this.rawRequest(
      'SELECT argMax(value, inserted_at) as value, argMax(name, inserted_at) as name ' +
      'FROM settings ' +
      `WHERE fingerprint IN (${fps.join(',')}) GROUP BY fingerprint HAVING name != '' FORMAT JSON`)
    if (!rows.data || !rows.data.data || !rows.data.data.length) {
      return {}
    }
    return rows.data.data.reduce((sum, cur) => {
      sum[cur.name] = cur.value
      return sum
    }, {})
  }

  /**
   *
   * @param type {string}
   * @param name {string}
   * @param value {string}
   * @returns {Promise<void>}
   */
  async addSetting (type, name, value) {
    await this.rawRequest('INSERT INTO settings (fingerprint, type, name, value, inserted_at) FORMAT JSONEachRow',
      JSON.stringify({
        fingerprint: this.getFP(type, name),
        type: type,
        name: name,
        value: value,
        inserted_at: formatISO9075(new Date())
      }) + '\n')
  }

  /**
   *
   * @param request {string}
   * @param data {string}
   * @param extraUrlParams {Object<string, string>}
   * @returns {Promise<AxiosResponse<any>>}
   */
  async rawRequest (request, data, extraUrlParams) {
    const url = new URL(this.getClickhouseUrl().toString())
    Object.entries(extraUrlParams || {}).forEach(e => url.searchParams.append(e[0], e[1]))
    if (!data) {
      return await axios.post(url.toString(), request)
    }
    url.searchParams.append('query', request)
    return await axios.post(url.toString(), data)
  }

  /**
   *
   * @param logQLReqs {string[]}
   * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
   * @returns {Promise<void>}
   */
  async scanSeries (logQLReqs, res) {
    const query = transpiler.transpileSeries(logQLReqs)
    const stream = await this.getClickhouseStream(query)
    res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
    const gen = StringStream.from(stream.data).lines().filter(r => r).map((row) => {
      try {
        return JSON.parse(JSON.parse(row).labels)
      } catch (e) {
        console.log(e)
        return null
      }
    }, DataStream).filter(row => row).toGenerator()
    res.res.write('{ "status": "success", "data": [')
    let i = 0
    for await (const item of gen()) {
      if (!item) {
        continue
      }
      if (i !== 0) {
        res.res.write(',')
      }
      res.res.write(JSON.stringify(item))
      i++
    }
    res.res.write(']}')
    res.res.end()
  }

  /**
   *
   * @returns {URL}
   */
  getClickhouseUrl () {
    return this.url
  }

  /**
   *
   * @param type {string}
   * @param name {string}
   */
  getFP (type, name) {
    return UTILS.fingerPrint(`${type}${name}`)
  }
}

module.exports.client = CLokiClient

module.exports.databaseOptions = clickhouseOptions
module.exports.cache = { bulk: bulk, bulk_labels: bulkLabels, labels: labels }
module.exports.capabilities = capabilities
module.exports.stop = () => {
  samplesThrottler.stop()
  timeSeriesThrottler.stop()
}
module.exports.ready = () => state === 'READY'
module.exports.samplesTableName = samplesTableName
module.exports.samplesReadTableName = samplesReadTableName
module.exports.getClickhouseUrl = getClickhouseUrl
