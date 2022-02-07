#!/usr/bin/env node

/*
 * Loki API to Clickhouse Gateway
 * (C) 2018-2022 QXIP BV
 */

this.debug = process.env.DEBUG || false
// const debug = this.debug

this.readonly = process.env.READONLY || false
this.http_user = process.env.CLOKI_LOGIN || false
this.http_password = process.env.CLOKI_PASSWORD || false

require('./plugins/engine')

const DATABASE = require('./lib/db/clickhouse')
const UTILS = require('./lib/utils')
const { EventEmitter } = require('events')

/* ProtoBuf Helpers */
const fs = require('fs')
const path = require('path')
const protoBuff = require('protocol-buffers')
const messages = protoBuff(fs.readFileSync('lib/loki.proto'))
const protobufjs = require('protobufjs')
const WriteRequest = protobufjs.loadSync(path.join(__dirname, 'lib/prompb.proto')).lookupType('WriteRequest')

/* Alerting */
const { startAlerting, stop } = require('./lib/db/alerting')
const yaml = require('yaml')
const { CLokiError } = require('./lib/handlers/errors')

/* Fingerprinting */
this.fingerPrint = UTILS.fingerPrint
this.toJSON = UTILS.toJSON

/* Database this.bulk Helpers */
this.bulk = DATABASE.cache.bulk // samples
this.bulk_labels = DATABASE.cache.bulk_labels // labels
this.labels = DATABASE.cache.labels // in-memory labels

/* Function Helpers */
this.labelParser = UTILS.labelParser

const init = DATABASE.init
this.reloadFingerprints = DATABASE.reloadFingerprints
this.scanFingerprints = DATABASE.scanFingerprints
this.instantQueryScan = DATABASE.instantQueryScan
this.tempoQueryScan = DATABASE.tempoQueryScan
this.scanMetricFingerprints = DATABASE.scanMetricFingerprints
this.tempoQueryScan = DATABASE.tempoQueryScan
this.scanClickhouse = DATABASE.scanClickhouse
let profiler = null

const shaper = {
  onParse: 0,
  onParsed: new EventEmitter(),
  shapeInterval: setInterval(() => {
    shaper.onParse = 0
    shaper.onParsed.emit('parsed')
  }, 1000),
  /**
   *
   * @param size {number}
   * @returns {Promise<void>}
   */
  register: async (size) => {
    while (shaper.onParse + size > 5e10) {
      await new Promise(resolve => { shaper.onParsed.once('parsed') })
    }
    shaper.onParse += size
  },
  stop: () => {
    shaper.shapeInterval && clearInterval(shaper.shapeInterval)
    shaper.shapeInterval = null
    shaper.onParsed.removeAllListeners('parsed')
    shaper.onParsed = null
  }
}

/**
 *
 * @param req {FastifyRequest}
 * @param limit {number}
 * @returns {number}
 */
function getContentLength (req, limit) {
  if (!req.headers['content-length'] || isNaN(parseInt(req.headers['content-length']))) {
    throw new CLokiError(400, 'Content-Length is required')
  }
  const res = parseInt(req.headers['content-length'])
  if (limit && res > limit) {
    throw new CLokiError(400, 'Request is too big')
  }
  return res
}

/**
 *
 * @param req {FastifyRequest}
 * @returns {Promise<string>}
 */
async function getContentBody (req) {
  let body = ''
  req.raw.on('data', data => {
    body += data.toString()
  })
  await new Promise(resolve => req.raw.once('end', resolve))
  return body
}

/**
 *
 * @param req {FastifyRequest}
 * @returns {Promise<void>}
 */
async function genericJSONParser (req) {
  try {
    const length = getContentLength(req, 1e9)
    if (req.routerPath === '/loki/api/v1/push' && length > 5e6) {
      return
    }
    await shaper.register(length)
    return JSON.parse(await getContentBody(req))
  } catch (err) {
    err.statusCode = 400
    throw err
  }
}

(async () => {
  if (!this.readonly) {
    await init(process.env.CLICKHOUSE_DB || 'cloki')
    await startAlerting()
  }
  if (!this.readonly && process.env.PROFILE) {
    const tag = JSON.stringify({ profiler_id: process.env.PROFILE, label: 'RAM usage' })
    const fp = this.fingerPrint(tag)
    profiler = setInterval(() => {
      this.bulk_labels.add([[new Date().toISOString().split('T')[0], fp, tag, '']])
      this.bulk.add([[fp, Date.now(), process.memoryUsage().rss / 1024 / 1024, '']])
    }, 1000)
  }
})().catch((err) => {
  console.log(err)
  process.exit(1)
})

/* Fastify Helper */
const fastify = require('fastify')({
  logger: false,
  requestTimeout: parseInt(process.env.FASTIFY_REQUESTTIMEOUT) || 0,
  maxRequestsPerSocket: parseInt(process.env.FASTIFY_MAXREQUESTS) || 0
})

fastify.register(require('fastify-url-data'))
fastify.register(require('fastify-websocket'))

/* CORS Helper */
const CORS = process.env.CORS_ALLOW_ORIGIN || '*'
fastify.register(require('fastify-cors'), {
  origin: CORS
})

fastify.after((err) => {
  if (err) throw err
})

/* Enable Simple Authentication */
if (this.http_user && this.http_password) {
  function checkAuth (username, password, req, reply, done) {
    if (username === this.http_user && password === this.http_password) {
      done()
    } else {
      done(new Error('Unauthorized!: Wrong username/password.'))
    }
  }
  const validate = checkAuth.bind(this)

  fastify.register(require('fastify-basic-auth'), {
    validate
  })
  fastify.after(() => {
    fastify.addHook('preHandler', fastify.basicAuth)
  })
}

fastify.addContentTypeParser('application/yaml', {},
  async function (req, body, done) {
    try {
      const length = getContentLength(req, 5e6)
      await shaper.register(length)
      const json = yaml.parse(await getContentBody(req))
      return json
    } catch (err) {
      err.statusCode = 400
      throw err
    }
  })

try {
  const snappy = require('snappyjs')
  /* Protobuf Handler */
  fastify.addContentTypeParser('application/x-protobuf', {},
    async function (req, body, done) {
      try {
        const length = getContentLength(req, 5e6)
        await shaper.register(length)
        let body = new Uint8Array()
        req.raw.on('data', (data) => {
          body = new Uint8Array([...body, ...Uint8Array.from(data)])
        })
        await new Promise(resolve => req.raw.once('end', resolve))
        // Prometheus Protobuf Write Handler
        if (req.url === '/api/v1/prom/remote/write') {
          let _data = await snappy.uncompress(body)
          _data = WriteRequest.decode(_data)
          _data.timeseries = _data.timeseries.map(s => ({
            ...s,
            samples: s.samples.map(e => {
              const millis = parseInt(e.timestamp.toNumber())
              return {
                ...e,
                timestamp: millis
              }
            })
          }))
          return _data
          // Loki Protobuf Push Handler
        } else {
          let _data = await snappy.uncompress(body)
          _data = messages.PushRequest.decode(Buffer.from(_data))
          _data.streams = _data.streams.map(s => ({
            ...s,
            entries: s.entries.map(e => {
              const millis = Math.floor(e.timestamp.nanos / 1000000)
              return {
                ...e,
                timestamp: e.timestamp.seconds * 1000 + millis
              }
            })
          }))
          return _data.streams
        }
      } catch (e) {
        console.log(e)
        throw e
      }
    })
} catch (e) {
  console.log(e)
  console.log('Protobuf ingesting is unsupported')
}

fastify.addContentTypeParser('application/json', {},
  async function (req, body, done) {
    return await genericJSONParser(req)
  })

/* Null content-type handler for CH-MV HTTP PUSH */
fastify.addContentTypeParser('*', {},
  async function (req, body, done) {
    return await genericJSONParser(req)
  })

/* 404 Handler */
const handler404 = require('./lib/handlers/404.js').bind(this)
fastify.setNotFoundHandler(handler404)
fastify.setErrorHandler(require('./lib/handlers/errors').handler.bind(this))

/* Hello cloki test API */
const handlerHello = require('./lib/handlers/ready').bind(this)
fastify.get('/hello', handlerHello)
fastify.get('/ready', handlerHello)

/* Write Handler */
const handlerPush = require('./lib/handlers/push.js').bind(this)
fastify.post('/loki/api/v1/push', handlerPush)

/* Tempo Write Handler */
this.tempo_tagtrace = process.env.TEMPO_TAGTRACE || false
const handlerTempoPush = require('./lib/handlers/tempo_push.js').bind(this)
fastify.post('/tempo/api/push', handlerTempoPush)
fastify.post('/api/v2/spans', handlerTempoPush)

/* Tempo Traces Query Handler */
this.tempo_span = process.env.TEMPO_SPAN || 24
const handlerTempoTraces = require('./lib/handlers/tempo_traces.js').bind(this)
fastify.get('/api/traces/:traceId', handlerTempoTraces)
fastify.get('/api/traces/:traceId/:json', handlerTempoTraces)

/* Tempo Tag Handlers */
const handlerTempoLabel = require('./lib/handlers/tags.js').bind(this)
fastify.get('/api/search/tags', handlerTempoLabel)

/* Tempo Tag Value Handler */
const handlerTempoLabelValues = require('./lib/handlers/tags_values.js').bind(this)
fastify.get('/api/search/tag/:name/values', handlerTempoLabelValues)

/* Telegraf HTTP Bulk handler */
const handlerTelegraf = require('./lib/handlers/telegraf.js').bind(this)
fastify.post('/telegraf', handlerTelegraf)

/* Query Handler */
const handlerQueryRange = require('./lib/handlers/query_range.js').bind(this)
fastify.get('/loki/api/v1/query_range', handlerQueryRange)

/* Label Handlers */
/* Label Value Handler via query (test) */
const handlerQuery = require('./lib/handlers/query.js').bind(this)
fastify.get('/loki/api/v1/query', handlerQuery)

/* Label Handlers */
const handlerLabel = require('./lib/handlers/label.js').bind(this)
fastify.get('/loki/api/v1/label', handlerLabel)
fastify.get('/loki/api/v1/labels', handlerLabel)

/* Label Value Handler */
const handlerLabelValues = require('./lib/handlers/label_values.js').bind(this)
fastify.get('/loki/api/v1/label/:name/values', handlerLabelValues)

/* Series Placeholder - we do not track this as of yet */
const handlerSeries = require('./lib/handlers/series.js').bind(this)
fastify.get('/loki/api/v1/series', handlerSeries)

fastify.get('/loki/api/v1/tail', { websocket: true }, require('./lib/handlers/tail').bind(this))

/* ALERT MANAGER Handlers */
fastify.get('/api/prom/rules', require('./lib/handlers/alerts/get_rules').bind(this))
fastify.get('/api/prom/rules/:ns/:group', require('./lib/handlers/alerts/get_group').bind(this))
fastify.post('/api/prom/rules/:ns', require('./lib/handlers/alerts/post_group').bind(this))
fastify.delete('/api/prom/rules/:ns/:group', require('./lib/handlers/alerts/del_group').bind(this))
fastify.delete('/api/prom/rules/:ns', require('./lib/handlers/alerts/del_ns').bind(this))
fastify.get('/prometheus/api/v1/rules', require('./lib/handlers/alerts/prom_get_rules').bind(this))

/* PROMETHEUS REMOTE WRITE Handlers */
fastify.post('/api/v1/prom/remote/write', require('./lib/handlers/prom_push.js').bind(this))
fastify.post('/api/prom/remote/write', require('./lib/handlers/prom_push.js').bind(this))

/* CLOKI-VIEW Optional Handler */
if (fs.existsSync(path.join(__dirname, 'view/index.html'))) {
  fastify.register(require('fastify-static'), {
    root: path.join(__dirname, 'view'),
    prefix: '/'
  })
}

// Run API Service
fastify.listen(
  process.env.PORT || 3100,
  process.env.HOST || '0.0.0.0',
  (err, address) => {
    if (err) throw err
    console.log('cLoki API up')
    fastify.log.info(`cloki API listening on ${address}`)
  }
)

module.exports.stop = () => {
  shaper.stop()
  profiler && clearInterval(profiler)
  fastify.close()
  DATABASE.stop()
  require('./parser/transpiler').stop()
  stop()
}
