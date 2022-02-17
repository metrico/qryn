/*
 * cLoki DB Adapter for Clickhouse
 * (C) 2018-2022 QXIP BV
 */

const UTILS = require('../utils')
const logger = require('../logger')
const toJSON = UTILS.toJSON
const { formatISO9075 } = require('date-fns')

/* DB Helper */
const ClickHouse = require('@apla/clickhouse')
const clickhouseOptions = {
  host: process.env.CLICKHOUSE_SERVER || 'localhost',
  port: process.env.CLICKHOUSE_PORT || 8123,
  auth: process.env.CLICKHOUSE_AUTH || 'default:',
  protocol: process.env.CLICKHOUSE_PROTO ? process.env.CLICKHOUSE_PROTO + ':' : 'http:',
  readonly: !!process.env.READONLY,
  queryOptions: { database: process.env.CLICKHOUSE_DB || 'cloki' }
}
const CORS = process.env.CORS_ALLOW_ORIGIN || '*'

const transpiler = require('../../parser/transpiler')
const rotationLabels = process.env.LABELS_DAYS || 7
const rotationSamples = process.env.SAMPLES_DAYS || 7
const axios = require('axios')
const { samplesTableName, samplesReadTableName } = UTILS
const path = require('path')

const protocol = process.env.CLICKHOUSE_PROTO || 'http'

// External Storage Policy for Tables (S3, MINIO)
const storagePolicy = process.env.STORAGE_POLICY || false

const { StringStream, DataStream } = require('scramjet')

const { parseLabels, hashLabels } = require('../../common')

const { Worker, isMainThread } = require('worker_threads')

const jsonSerializer = (k, val) => typeof val === 'bigint' ? val.toString() : val

const capabilities = {}
let state = 'INITIALIZING'

const clickhouse = new ClickHouse(clickhouseOptions)
let ch

let throttler = null
const resolvers = {}
const rejectors = {}
if (isMainThread) {
  throttler = new Worker(path.join(__dirname, 'throttler.js'))
  throttler.on('message', (msg) => {
    switch (msg.status) {
      case 'ok':
        resolvers[msg.id]()
        break
      case 'err':
        rejectors[msg.id](new Error('Database push error'))
        break
    }
    delete resolvers[msg.id]
    delete rejectors[msg.id]
  })
}

// timeSeriesv2Throttler.start();

/* Cache Helper */
const recordCache = require('record-cache')
const { parseMs } = require('../utils')
let id = 0
// Flushing to Clickhouse
const bulk = {
  add: (values) => {
    id = id + 1 % 1e6
    return new Promise((resolve, reject) => {
      throttler.postMessage({
        type: 'values',
        data: values.map(r => JSON.stringify({
          fingerprint: r[0],
          timestamp_ms: r[1],
          value: r[2],
          string: r[3]
        }, jsonSerializer)).join('\n'),
        id: id
      })
      resolvers[id] = resolve
      rejectors[id] = reject
    })
  }
}

const bulkLabels = {
  add: (values) => {
    return new Promise((resolve, reject) => {
      id = id + 1 % 1e6
      throttler.postMessage({
        type: 'labels',
        data: values.map(r => JSON.stringify({
          date: r[0],
          fingerprint: r[1],
          labels: r[2],
          name: r[3]
        }, jsonSerializer)).join('\n'),
        id: id
      })
      resolvers[id] = resolve
      rejectors[id] = reject
    })
  }
}

// In-Memory LRU for quick lookups
const labels = recordCache({
  maxSize: process.env.BULK_MAXCACHE || 50000,
  maxAge: 0,
  onStale: false
})

/* Initialize */
const initialize = async function (dbName) {
  logger.info('Initializing DB... ' + dbName)
  const tmp = { ...clickhouseOptions, queryOptions: { database: '' } }
  ch = new ClickHouse(tmp)
  const maintain = require('./maintain/index')
  await maintain.upgrade(dbName)
  await maintain.rotate([{
    db: dbName,
    samples_days: rotationSamples,
    time_series_days: rotationLabels,
    storage_policy: storagePolicy
  }])
  await checkCapabilities()
  await samplesReadTable.check()

  state = 'READY'

  reloadFingerprints()
}

const checkCapabilities = async () => {
  logger.info('Checking clickhouse capabilities')
  try {
    await axios.post(getClickhouseUrl() + '/?allow_experimental_live_view=1',
      `CREATE LIVE VIEW ${clickhouseOptions.queryOptions.database}.lvcheck WITH TIMEOUT 1 AS SELECT 1`)
    capabilities.liveView = true
    logger.info('LIVE VIEW: supported')
  } catch (e) {
    logger.info('LIVE VIEW: unsupported')
    capabilities.liveView = false
  }
}

const reloadFingerprints = function () {
  logger.info('Reloading Fingerprints...')
  const selectQuery = `SELECT DISTINCT fingerprint, labels FROM ${clickhouseOptions.queryOptions.database}.time_series`
  const stream = ch.query(selectQuery)
  // or collect records yourself
  const rows = []
  stream.on('metadata', function (columns) {
    // do something with column list
  })
  stream.on('data', function (row) {
    rows.push(row)
  })
  stream.on('error', function (err) {
    logger.error({ err }, 'Error reloading fingerprints')
  })
  stream.on('end', function () {
    rows.forEach(function (row) {
      try {
        const JSONLabels = toJSON(row[1].replace(/\!?=/g, ':'))
        labels.add(row[0], JSON.stringify(JSONLabels))
        for (const key in JSONLabels) {
          // logger.debug('Adding key',row);
          labels.add('_LABELS_', key)
          labels.add(key, JSONLabels[key])
        }
      } catch (err) { logger.error({ err }, 'error reloading fingerprints') }
    })
  })
}

const fakeStats = { summary: { bytesProcessedPerSecond: 0, linesProcessedPerSecond: 0, totalBytesProcessed: 0, totalLinesProcessed: 0, execTime: 0.001301608 }, store: { totalChunksRef: 0, totalChunksDownloaded: 0, chunksDownloadTime: 0, headChunkBytes: 0, headChunkLines: 0, decompressedBytes: 0, decompressedLines: 0, compressedBytes: 0, totalDuplicates: 0 }, ingester: { totalReached: 1, totalChunksMatched: 0, totalBatches: 0, totalLinesSent: 0, headChunkBytes: 0, headChunkLines: 0, decompressedBytes: 0, decompressedLines: 0, compressedBytes: 0, totalDuplicates: 0 } }

const scanFingerprints = async function (query, res) {
  logger.debug('Scanning Fingerprints...')
  const _query = transpiler.transpile(query)
  _query.step = UTILS.parseOrDefault(query.step, 5) * 1000
  return queryFingerprintsScan(_query, res)
}

const instantQueryScan = async function (query, res) {
  logger.debug('Scanning Fingerprints...')
  const time = parseMs(query.time, Date.now())
  query.start = (time - 10 * 60 * 1000) * 1000000
  query.end = Date.now() * 1000000
  const _query = transpiler.transpile(query)
  _query.step = UTILS.parseOrDefault(query.step, 5) * 1000

  const _stream = await axios.post(getClickhouseUrl() + '/',
    _query.query + ' FORMAT JSONEachRow',
    {
      responseType: 'stream'
    }
  )
  const dataStream = preprocessStream(_stream, _query.stream || [])
  return await (_query.matrix
    ? outputQueryVector(dataStream, res)
    : outputQueryStreams(dataStream, res))
}

const tempoQueryScan = async function (query, res, traceId) {
  logger.debug(`Scanning Tempo Fingerprints... ${traceId}`)
  const time = parseMs(query.time, Date.now())
  /* Tempo does not seem to pass start/stop parameters. Use ENV or default 24h */
  const hours = this.tempo_span || 24
  if (!query.start) query.start = (time - (hours * 60 * 60 * 1000)) * 1000000
  if (!query.end) query.end = Date.now() * 1000000
  const _query = transpiler.transpile(query)
  _query.step = UTILS.parseOrDefault(query.step, 5) * 1000

  const _stream = await axios.post(getClickhouseUrl() + '/',
    _query.query + ' FORMAT JSONEachRow',
    {
      responseType: 'stream'
    }
  )
  const dataStream = preprocessStream(_stream, _query.stream || [])
  logger.info('debug tempo', query)
  return await (outputTempoSpans(dataStream, res, traceId))
}

function getClickhouseUrl () {
  return `${protocol}://${clickhouseOptions.auth}@${clickhouseOptions.host}:${clickhouseOptions.port}`
}

/**
 * @param query {
 * {query: string, duration: number, matrix: boolean, stream: (function(DataStream): DataStream)[], step: number}
 * }
 * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
 * @returns {Promise<void>}
 */
const queryFingerprintsScan = async function (query, res) {
  logger.debug('Scanning Fingerprints...')

  // logger.info(_query.query);
  const _stream = await getClickhouseStream(query)
  const dataStream = preprocessStream(_stream, query.stream || [])
  return await (query.matrix
    ? outputQueryMatrix(dataStream, res, query.step, query.duration)
    : outputQueryStreams(dataStream, res))
}

/**
 *
 * @param query {{query: string}}
 * @returns {Promise<Stream>}
 */
const getClickhouseStream = (query) => {
  return axios.post(getClickhouseUrl() + '/',
    query.query + ' FORMAT JSONEachRow',
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
const outputQueryStreams = async (dataStream, res) => {
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
const outputQueryMatrix = async (dataStream, res,
  stepMs, durationMs) => {
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
const outputQueryVector = async (dataStream, res) => {
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
const outputTempoSpans = async (dataStream, res, traceId) => {
  // res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
  const gen = dataStream.toGenerator()
  let i = 0
  let lastLabels = null
  let lastStream = []
  let response = '{"total": 0, "limit": 0, "offset": 0, "errors": null, "processes" : { "p1": {} }, "data": [ { "traceID": "' +
    traceId + '", '
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
const preprocessStream = (rawStream, processors) => {
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
        logger.info(chunk)
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

/**
 *
 * @param rawStream {any} Stream from axios response
 * @param processors {(function(DataStream): DataStream)[] | undefined}
 * @returns {DataStream}
 */
const preprocessLiveStream = (rawStream, processors) => {
  let dStream = StringStream.from(rawStream.data).lines().endWith(JSON.stringify({ EOF: true }))
    .map(chunk => chunk ? JSON.parse(chunk) : ({}), DataStream)
    .filter(chunk => {
      return chunk && (chunk.row || chunk.EOF)
    }).map(chunk => ({
      ...(chunk.row || {}),
      EOF: chunk.EOF
    }))
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
        logger.info(chunk)
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
const scanMetricFingerprints = function (settings, client, params) {
  logger.debug({ settings }, 'Scanning Clickhouse...')
  // populate matrix structure
  const resp = {
    status: 'success',
    data: {
      resultType: 'matrix',
      result: []
    }
  }
  // Check for required fields or return nothing!
  if (!settings || !settings.table || !settings.db || !settings.tag || !settings.metric) { client.send(resp); return }
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
  // } else {
  //  template += " timestampMs, value"
  // }

  // template += " timestampMs, value"
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
    logger.debug({ rows }, 'CLICKHOUSE RESPONSE')
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
      } catch (err) { logger.error({ err }, 'Error scanning fingerprints') }
    }
    logger.debug({ resp }, 'CLOKI RESPONSE')
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
const scanClickhouse = function (settings, client, params) {
  logger.debug('Scanning Clickhouse...', settings)

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
  if (!settings || !settings.table || !settings.db || !settings.tag || !settings.metric) { client.send(resp); return }
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
  // Read-Only: Initiate a new driver connection
  if (process.env.READONLY) {
    const tmp = { ...clickhouseOptions, queryOptions: { database: settings.db } }
    ch = new ClickHouse(tmp)
  }

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
    logger.debug({ rows }, 'CLICKHOUSE RESPONSE')
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
      } catch (err) { logger.error({ err }, 'error scanning clickhouse') }
    }
    logger.debug({ resp }, 'CLOKI RESPONSE')
    client.send(resp)
  })
}

/**
 *
 * @param matches {string[]} ['{ts1="a1"}', '{ts2="a2"}', ...]
 * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
 */
const getSeries = async (matches, res) => {
  const query = transpiler.transpileSeries(matches)
  const stream = await axios.post(`${getClickhouseUrl()}`, query + ' FORMAT JSONEachRow', {
    responseType: 'stream'
  })
  const dStream = StringStream.from(stream.data).lines().map(l => {
    if (!l) {
      return null
    }
    try {
      return JSON.parse(l)
    } catch (err) {
      logger.error({ line: l, err }, 'Error parsing line')
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

const ping = async () => {
  await Promise.all([
    new Promise((resolve, reject) => ch.query('SELECT 1', undefined, (err) => {
      err ? reject(err) : resolve(err)
    })),
    axios.get(`${getClickhouseUrl()}/?query=SELECT 1`)
  ])
}

/* Module Exports */

/**
 *
 * @param name {string}
 * @param request {string}
 * @param options {{db : string | undefined, timeout_sec: number | undefined}}
 */
module.exports.createLiveView = (name, request, options) => {
  const db = options.db || clickhouseOptions.queryOptions.database
  const timeout = options.timeout_sec ? `WITH TIMEOUT ${options.timeout_sec}` : ''
  return axios.post(`${getClickhouseUrl()}/?allow_experimental_live_view=1`,
    `CREATE LIVE VIEW ${db}.${name} ${timeout} AS ${request}`)
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
module.exports.watchLiveView = async (name, db, res, options) => {
  db = db || clickhouseOptions.queryOptions.database
  const cancel = axios.CancelToken.source()
  const stream = await axios.post(`${getClickhouseUrl()}/?allow_experimental_live_view=1`,
    `WATCH ${db}.${name} FORMAT JSONEachRowWithProgress`,
    {
      responseType: 'stream',
      cancelToken: cancel.token
    })
  const endPromise = (async () => {
    const _stream = preprocessLiveStream(stream, options.stream)
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

module.exports.createMV = async (query, id, url) => {
  const request = `CREATE MATERIALIZED VIEW ${clickhouseOptions.queryOptions.database}.${id} ` +
    `ENGINE = URL('${url}', JSON) AS ${query}`
  logger.info(`MV: ${request}`)
  await axios.post(`${getClickhouseUrl()}`, request)
}

const samplesReadTable = {
  checked: false,
  v1: false,
  v1Time: false,
  getName: (fromMs) => {
    if (!samplesReadTable.checked) {
      return 'samples_read'
    }
    if (!samplesReadTable.v1) {
      return 'samples_v3'
    }
    if (!fromMs || fromMs < samplesReadTable.v1Time) {
      return 'samples_read'
    }
    return 'samples_v2'
  },
  check: async function () {
    await this._check('samples_v2')
    if (samplesReadTable.v1) {
      return
    }
    await this._check('samples')
  },
  _check: async function(tableName) {
    try {
      logger.info('checking old samples support: ' + tableName)
      samplesReadTable.checked = true
      console.log(`${getClickhouseUrl()}/?database=${UTILS.DATABASE_NAME()}`)
      console.log('show tables format JSON')
      const tablesResp = await axios.post(`${getClickhouseUrl()}/?database=${UTILS.DATABASE_NAME()}`,
        'show tables format JSON')
      samplesReadTable.v1 = tablesResp.data.data.find(row => row.name === tableName)
      if (!samplesReadTable.v1) {
        return
      }
      logger.info('checking last timestamp')
      const v1EndTime = await axios.post(`${getClickhouseUrl()}/?database=${UTILS.DATABASE_NAME()}`,
        `SELECT max(timestamp_ms) as ts FROM ${tableName} format JSON`)
      if (!v1EndTime.data.rows) {
        samplesReadTable.v1 = false
        return
      }
      samplesReadTable.v1 = true
      samplesReadTable.v1Time = parseInt(v1EndTime.data.data[0].ts)
    } catch (e) {
      logger.error(e.message)
      logger.error(e.stack)
      samplesReadTable.v1 = false
      logger.info('old samples table not supported')
    } finally {
      UTILS._samplesReadTableName = samplesReadTable.getName
    }
  }
}

/**
 *
 * @param query {string}
 * @param data {string | Buffer | Uint8Array}
 * @param database {string}
 * @returns {Promise<AxiosResponse<any>>}
 */
const rawRequest = (query, data, database) => {
  const getParams = [
    (database ? `database=${encodeURIComponent(database)}` : null),
    (data ? `query=${encodeURIComponent(query)}` : null)
  ].filter(p => p)
  const url = `${getClickhouseUrl()}/${getParams.length ? `?${getParams.join('&')}` : ''}`
  return axios.post(url, data || query)
}

/**
 *
 * @param names {{type: string, name: string}[]}
 * @param database {string}
 * @returns {Promise<Object<string, string>>}
 */
const getSettings = async (names, database) => {
  const fps = names.map(n => UTILS.fingerPrint(JSON.stringify({ type: n.type, name: n.name }), false,
    'short-hash'))
  const settings = await rawRequest(`SELECT argMax(name, inserted_at) as _name, 
        argMax(value, inserted_at) as _value
        FROM settings WHERE fingerprint IN (${fps.join(',')}) HAVING _name != '' GROUP BY fingerprint FORMAT JSON`,
  null, database)
  return settings.data.data.reduce((sum, cur) => {
    sum[cur._name] = cur._value
    return sum
  }, {})
}

/**
 *
 * @param type {string}
 * @param name {string}
 * @param value {string}
 * @param database {string}
 * @returns {Promise<void>}
 */
const addSetting = async (type, name, value, database) => {
  const fp = UTILS.fingerPrint(JSON.stringify({ type: type, name: name }), 'short-hash')
  return rawRequest('INSERT INTO settings (fingerprint, type, name, value, inserted_at) FORMAT JSONEachRow',
    JSON.stringify({
      fingerprint: fp,
      type: type,
      name: name,
      value: value,
      inserted_at: formatISO9075(new Date())
    }) + '\n')
}

module.exports.samplesReadTable = samplesReadTable
module.exports.databaseOptions = clickhouseOptions
module.exports.database = clickhouse
module.exports.cache = { bulk: bulk, bulk_labels: bulkLabels, labels: labels }
module.exports.scanFingerprints = scanFingerprints
module.exports.queryFingerprintsScan = queryFingerprintsScan
module.exports.instantQueryScan = instantQueryScan
module.exports.tempoQueryScan = tempoQueryScan
module.exports.scanMetricFingerprints = scanMetricFingerprints
module.exports.scanClickhouse = scanClickhouse
module.exports.reloadFingerprints = reloadFingerprints
module.exports.init = initialize
module.exports.preprocessStream = preprocessStream
module.exports.capabilities = capabilities
module.exports.ping = ping
module.exports.stop = () => {
  throttler.postMessage({ type: 'end' })
  throttler.removeAllListeners('message')
  throttler.terminate()
}
module.exports.ready = () => state === 'READY'
module.exports.scanSeries = getSeries
module.exports.samplesTableName = samplesTableName
module.exports.samplesReadTableName = samplesReadTableName
module.exports.getClickhouseUrl = getClickhouseUrl
module.exports.getClickhouseStream = getClickhouseStream
module.exports.preprocessLiveStream = preprocessLiveStream
module.exports.rawRequest = rawRequest
module.exports.getSettings = getSettings
module.exports.addSetting = addSetting
