/*
 * cLoki DB Adapter for Clickhouse
 * (C) 2018-2019 QXIP BV
 */

const debug = process.env.DEBUG || false
const UTILS = require('../utils')
const toJSON = UTILS.toJSON

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
// clickhouseOptions.queryOptions.database = process.env.CLICKHOUSE_DB || 'cloki';

const transpiler = require('../../parser/transpiler')
const rotationLabels = process.env.LABELS_DAYS || 7
const rotationSamples = process.env.SAMPLES_DAYS || 7
const axios = require('axios')

const protocol = process.env.CLICKHOUSE_PROTO || 'http'

// External Storage Policy for Tables (S3, MINIO)
const storagePolicy = process.env.STORAGE_POLICY || false

const { StringStream, DataStream } = require('scramjet')

const { parseLabels, hashLabels } = require('../../common')

const capabilities = {}
let state = 'INITIALIZING'

const clickhouse = new ClickHouse(clickhouseOptions)
let ch

const samples = []
const timeSeries = []

class TimeoutThrottler {
  constructor (statement) {
    this.statement = statement
    this.on = false
    this.queue = []
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
          await new Promise((resolve, reject) => setTimeout(resolve, 100 - p))
        }
      }
    })
  }

  async flush () {
    const len = this.queue.length
    if (len < 1) {
      return
    }
    const ts = Date.now()
    const resp = await axios.post(`${getClickhouseUrl()}/?query=${this.statement}`,
      this.queue.join('\n')
    )
    this.queue = this.queue.slice(len)
  }

  stop () {
    this.on = false
  }
}

const samplesThrottler = new TimeoutThrottler(
    `INSERT INTO ${clickhouseOptions.queryOptions.database}.samples(fingerprint, timestamp_ms, value, string) FORMAT JSONEachRow`)
const timeSeriesThrottler = new TimeoutThrottler(
    `INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series(date, fingerprint, labels, name) FORMAT JSONEachRow`)
/* TODO: tsv2
const timeSeriesv2Throttler = new TimeoutThrottler(
	`INSERT INTO ${clickhouseOptions.queryOptions.database}.time_series_v2(date, fingerprint, labels, name) FORMAT JSONEachRow`); */
samplesThrottler.start()
timeSeriesThrottler.start()
// timeSeriesv2Throttler.start();

/* Cache Helper */
const recordCache = require('record-cache')
const onStale = function (data) {
 	for (const [key, value] of data.records.entries()) {
    samplesThrottler.queue.push.apply(samplesThrottler.queue, value.list.map(r => JSON.stringify({
      fingerprint: r.record[0],
      timestamp_ms: r.record[1],
      value: r.record[2],
      string: r.record[3]
    })))
  }
}
const onStale_labels = function (data) {
 	for (const [key, value] of data.records.entries()) {
	     timeSeriesThrottler.queue.push.apply(timeSeriesThrottler.queue, value.list.map(r => JSON.stringify({
      date: r.record[0],
      fingerprint: r.record[1],
      labels: r.record[2],
      name: r.record[3]
    })))
	    /* TODO: tsv2
		timeSeriesv2Throttler.queue.push.apply(timeSeriesv2Throttler.queue, value.list.map(r => JSON.stringify({
			date: r.record[0],
			fingerprint: r.record[1],
			labels: JSON.parse(r.record[2]),
			name: r.record[3]
		})));
		*/
  }
}

// Flushing to Clickhouse
const bulk = recordCache({
  maxSize: process.env.BULK_MAXSIZE || 5000,
  maxAge: process.env.BULK_MAXAGE || 2000,
  onStale: onStale
})

const bulk_labels = recordCache({
  maxSize: 100,
  maxAge: 500,
  onStale: onStale_labels
})

// In-Memory LRU for quick lookups
const labels = recordCache({
  maxSize: process.env.BULK_MAXCACHE || 50000,
  maxAge: 0,
  onStale: false
})

/* Initialize */
const initialize = function (dbName) {
  console.log('Initializing DB...', dbName)
  const dbQuery = 'CREATE DATABASE IF NOT EXISTS ' + dbName
  const tmp = { ...clickhouseOptions, queryOptions: { database: '' } }
  ch = new ClickHouse(tmp)

  const hack_ch = (ch) => {
    ch._query = ch.query
    ch.query = (q, opts, cb) => {
      return new Promise(f => ch._query(q, opts, (err, data) => {
        cb(err, data)
        f()
      }))
    }
  }

  ch.query(dbQuery, undefined, async function (err, data) {
    if (err) { console.error('error', err); return }
    const ch = new ClickHouse(clickhouseOptions)
    hack_ch(ch)
    console.log('CREATE TABLES', dbName)

    let ts_table = 'CREATE TABLE IF NOT EXISTS ' + dbName + '.time_series (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint'
    let sm_table = 'CREATE TABLE IF NOT EXISTS ' + dbName + '.samples (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (fingerprint, timestamp_ms)'

    if (storagePolicy) {
      console.log('ADD SETTINGS storage policy', storagePolicy)
      const set_storage = ` SETTINGS storagePolicy='${storagePolicy}'`
      ts_table += set_storage
      sm_table += set_storage
    }

	  	await ch.query(ts_table, undefined, function (err, data) {
      if (err) { console.log(err); process.exit(1) } else if (debug) console.log('Timeseries Table ready!')
      console.log('Timeseries Table ready!')
      return true
    })
	  	await ch.query(sm_table, undefined, function (err, data) {
      if (err) { console.log(err); process.exit(1) } else if (debug) console.log('Samples Table ready!')
      return true
    })

    var alter_table = 'ALTER TABLE ' + dbName + '.samples MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
    var rotate_table = 'ALTER TABLE ' + dbName + '.samples MODIFY TTL toDateTime(timestamp_ms / 1000)  + INTERVAL ' + rotationSamples + ' DAY'

	  	await ch.query(alter_table, undefined, function (err, data) {
      if (err) { console.log(err) } else if (debug) console.log('Samples Table altered for rotation!')
      // return true;
    })
	  	await ch.query(rotate_table, undefined, function (err, data) {
      if (err) { console.log(err) } else if (debug) console.log('Samples Table rotation set to days: ' + rotationSamples)
      return true
    })

    var alter_table = 'ALTER TABLE ' + dbName + '.time_series MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
    var rotate_table = 'ALTER TABLE ' + dbName + '.time_series MODIFY TTL date  + INTERVAL ' + rotationLabels + ' DAY'

	  	await ch.query(alter_table, undefined, function (err, data) {
      if (err) { console.log(err) } else if (debug) console.log('Labels Table altered for rotation!')
      return true
    })
	  	await ch.query(rotate_table, undefined, function (err, data) {
      if (err) { console.log(err) } else if (debug) console.log('Labels Table rotation set to days: ' + rotationLabels)
      return true
    })

    if (storagePolicy) {
      console.log('ALTER storage policy', storagePolicy)
      const alter_ts = `ALTER TABLE ${dbName}.time_series MODIFY SETTING storagePolicy='${storagePolicy}'`
      const alter_sm = `ALTER TABLE ${dbName}.samples MODIFY SETTING storagePolicy='${storagePolicy}'`

      await ch.query(alter_ts, undefined, function (err, data) {
        if (err) { console.log(err) } else if (debug) console.log('Storage policy update for fingerprints ' + storagePolicy)
        return true
      })
      await ch.query(alter_sm, undefined, function (err, data) {
        if (err) { console.log(err) } else if (debug) console.log('Storage policy update for samples ' + storagePolicy)
        return true
      })
    }

    await checkCapabilities()

    state = 'READY'

    /* TODO: tsv2
		const tsv2 = await axios.get(`${protocol}://${clickhouseOptions.auth}@${clickhouseOptions.host}:${clickhouseOptions.port}/?query=SHOW TABLES FROM ${dbName} LIKE 'time_series_v2' FORMAT JSON`);
		if (!tsv2.data.rows) {
			const create_tsv2 = `CREATE TABLE IF NOT EXISTS ${dbName}.time_series_v2
				(
				    date Date,
				    fingerprint UInt64,
					labels Array(Tuple(String, String)),
				    labels_map Map(String, String),
    				name String
    			) ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint`;
			await ch.query(create_tsv2, undefined, () => {});
			const insert = `INSERT INTO ${dbName}.time_series_v2 (date, fingerprint, labels, labels_map, name)
				SELECT date, fingerprint, JSONExtractKeysAndValues(labels, 'String') as labels,
				  CAST((
  					arrayMap(x -> x.1, JSONExtractKeysAndValues(labels, 'String')),
  					arrayMap(x -> x.2, JSONExtractKeysAndValues(labels, 'String'))), 'Map(String, String)') as labels_map,
  					name FROM ${dbName}.time_series`;
			await axios.post(`${protocol}://${clickhouseOptions.auth}@${clickhouseOptions.host}:${clickhouseOptions.port}/`,
				insert);
		} */

    reloadFingerprints()
  })
}

const checkCapabilities = async () => {
  console.log('Checking clickhouse capabilities: ')
  try {
    await axios.post(getClickhouseUrl() + '/?allow_experimental_live_view=1',
			`CREATE LIVE VIEW ${clickhouseOptions.queryOptions.database}.lvcheck WITH TIMEOUT 1 AS SELECT 1`)
    capabilities.liveView = true
    console.log('LIVE VIEW: supported')
  } catch (e) {
    console.log('LIVE VIEW: unsupported')
    capabilities.liveView = false
  }
}

var reloadFingerprints = function () {
  console.log('Reloading Fingerprints...')
  const select_query = `SELECT DISTINCT fingerprint, labels FROM ${clickhouseOptions.queryOptions.database}.time_series`
  const stream = ch.query(select_query)
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
  })
  stream.on('end', function () {
	  rows.forEach(function (row) {
      try {
        const JSON_labels = toJSON(row[1].replace(/\!?=/g, ':'))
        labels.add(row[0], JSON.stringify(JSON_labels))
        for (const key in JSON_labels) {
          // if (debug) console.log('Adding key',row);
          labels.add('_LABELS_', key)
          labels.add(key, JSON_labels[key])
        };
      } catch (e) { console.error(e) }
	  })
	  if (debug) console.log('Reloaded fingerprints:', rows.length + 1)
  })
}

const fakeStats = { summary: { bytesProcessedPerSecond: 0, linesProcessedPerSecond: 0, totalBytesProcessed: 0, totalLinesProcessed: 0, execTime: 0.001301608 }, store: { totalChunksRef: 0, totalChunksDownloaded: 0, chunksDownloadTime: 0, headChunkBytes: 0, headChunkLines: 0, decompressedBytes: 0, decompressedLines: 0, compressedBytes: 0, totalDuplicates: 0 }, ingester: { totalReached: 1, totalChunksMatched: 0, totalBatches: 0, totalLinesSent: 0, headChunkBytes: 0, headChunkLines: 0, decompressedBytes: 0, decompressedLines: 0, compressedBytes: 0, totalDuplicates: 0 } }

const scanFingerprints = async function (query, res) {
  if (debug) console.log('Scanning Fingerprints...')
  const _query = transpiler.transpile(query)
  _query.step = UTILS.parseOrDefault(query.step, 5) * 1000
  return queryFingerprintsScan(_query, res)
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
var queryFingerprintsScan = async function (query, res) {
  if (debug) console.log('Scanning Fingerprints...')

  // console.log(_query.query);
  const _stream = await axios.post(getClickhouseUrl() + '/',
    query.query + ' FORMAT JSONEachRow',
    {
      responseType: 'stream'
    }
  )
  return await processResponseStream(query, _stream, res)
}

/**
 * @param query {
 * {duration: number, matrix: boolean, stream: (function(DataStream): DataStream)[], step: number}
 * }
 * @param _stream {any} Stream returned by axios stream req
 * @param res {{res: {write: (function(string)), writeHead: (function(number, {}))}}}
 * @returns {Promise<void>}
 */
const processResponseStream = async (query, _stream, res) => {
  let lastLabel = null
  let stream = []
  let i = 0
  res.res.writeHead(200, { 'Content-Type': 'application/json' })
  /**
  *
  * @param s {DataStream}
  */
  let process = (s) => s
  if (!query.matrix) {
    process = (s) => s.remap((emit, row) => {
      if (lastLabel && !row.labels) {
        emit({
          stream: parseLabels(lastLabel),
          values: stream
        })
      } else if (lastLabel && hashLabels(row.labels) !== hashLabels(lastLabel)) {
        emit({
          stream: parseLabels(lastLabel),
          values: stream
        })
        stream = []
      }
      lastLabel = row.labels
      row.timestamp_ms && stream.push([(parseInt(row.timestamp_ms) * 1000000).toString(), row.string])
    })
  } else {
    const step = query.step || 5000
    const duration = query.duration
    let nextTime = 0
    process = (s) => s.remap((emit, row) => {
      if (lastLabel && (!row.labels || hashLabels(row.labels) !== hashLabels(lastLabel))) {
        if (stream.length === 1) {
          stream.push([stream[0][0] + (step / 1000), stream[0][1]])
        }
        emit({
          metric: lastLabel ? parseLabels(lastLabel) : {},
          values: stream
        })
        lastLabel = null
        stream = []
        nextTime = 0
      }
      if (!row.labels) {
        return
      }

      lastLabel = row.labels
      const timestampMs = parseInt(row.timestamp_ms)
      if (timestampMs < nextTime) {
        return
      }
      for (let ts = timestampMs; ts < timestampMs + duration; ts += step) {
        stream.push([ts / 1000, row.value + ''])
      }
      nextTime = timestampMs + Math.max(duration, step)
    })
  }
  const dStream = preprocessStream(_stream, query.stream)

  const gen = process(dStream).toGenerator()
  res.res.write(`{"status":"success", "data":{ "resultType":"${query.matrix ? 'matrix' : 'streams'}", "result": [`)
  for await (const item of gen()) {
    if (!item) {
      continue
    }
    res.res.write((i === 0 ? '' : ',') + JSON.stringify(item))
    ++i
  }
  res.res.write(']}}')
  res.res.end()
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
const scanMetricFingerprints = function (settings, client, params) {
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

/* Clickhouse Metrics Column Query */
const scanClickhouse = function (settings, client, params) {
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
  if (!settings || !settings.table || !settings.db || !settings.tag || !settings.metric) { client.send(resp); return }
  settings.interval = settings.interval ? parseInt(settings.interval) : 60
  if (!settings.timefield) settings.timefield = process.env.TIMEFIELD || 'record_datetime'

  // Lets query!
  let template = 'SELECT ' + settings.tag + ', groupArray((t, c)) AS groupArr FROM (' +
    'SELECT (intDiv(toUInt32(' + settings.timefield + '), ' + settings.interval + ') * ' + settings.interval + ') * 1000 AS t, ' + settings.tag + ', ' + settings.metric + ' c ' +
    'FROM ' + settings.db + '.' + settings.table
  if (params.start && params.end) {
    template += ' PREWHERE ' + settings.timefield + ' BETWEEN ' + parseInt(params.start / 1000000000) + ' AND ' + parseInt(params.end / 1000000000)
  }
  if (settings.where) {
    template += ' AND ' + settings.where
  }
  template += ' GROUP BY t, ' + settings.tag + ' ORDER BY t, ' + settings.tag + ')'
  template += ' GROUP BY ' + settings.tag + ' ORDER BY ' + settings.tag
  if (debug) console.log('CLICKHOUSE SEARCH QUERY', template)

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
    } catch (e) {
      console.log(l)
      console.log(e)
      return null
    }
  }, DataStream).filter(e => e)
  const gen = dStream.toGenerator()
  res.res.writeHead(200, { 'Content-Type': 'application/json' })
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
    `WATCH ${db}.${name} FORMAT JSONEachRow`,
    {
      responseType: 'stream',
      cancelToken: cancel.token
    })
  const endPromise = (async () => {
    const _stream = preprocessStream(stream, options.stream)
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

module.exports.databaseOptions = clickhouseOptions
module.exports.database = clickhouse
module.exports.cache = { bulk: bulk, bulk_labels: bulk_labels, labels: labels }
module.exports.scanFingerprints = scanFingerprints
module.exports.queryFingerprintsScan = queryFingerprintsScan
module.exports.scanMetricFingerprints = scanMetricFingerprints
module.exports.scanClickhouse = scanClickhouse
module.exports.reloadFingerprints = reloadFingerprints
module.exports.init = initialize
module.exports.capabilities = capabilities
module.exports.ping = ping
module.exports.stop = () => {
  samplesThrottler.stop()
  timeSeriesThrottler.stop()
}
module.exports.ready = () => state === 'READY'
module.exports.scanSeries = getSeries
