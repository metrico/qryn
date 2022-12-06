#!/usr/bin/env node

/*
 * LogQL API to Clickhouse Gateway
 * (C) 2018-2022 QXIP BV
 */

this.readonly = process.env.READONLY || false
this.http_user = process.env.QRYN_LOGIN || process.env.CLOKI_LOGIN || undefined
this.http_password = process.env.QRYN_PASSWORD || process.env.CLOKI_PASSWORD || undefined

this.maxListeners = process.env.MAXLISTENERS || 0;
process.setMaxListeners(this.maxListeners)

require('./plugins/engine')

const DATABASE = require('./lib/db/clickhouse')
const UTILS = require('./lib/utils')

/* ProtoBuf Helpers */
const fs = require('fs')
const path = require('path')

const logger = require('./lib/logger')

/* Alerting */
const { startAlerting, stop } = require('./lib/db/alerting')

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
this.scanTempo = DATABASE.scanTempo
this.instantQueryScan = DATABASE.instantQueryScan
this.tempoQueryScan = DATABASE.tempoQueryScan
this.scanMetricFingerprints = DATABASE.scanMetricFingerprints
this.tempoQueryScan = DATABASE.tempoQueryScan
this.scanClickhouse = DATABASE.scanClickhouse
this.pushZipkin = DATABASE.pushZipkin
this.pushOTLP = DATABASE.pushOTLP
this.queryTempoTags = DATABASE.queryTempoTags
this.queryTempoValues = DATABASE.queryTempoValues
let profiler = null

const {
  shaper,
  parsers,
  lokiPushJSONParser, lokiPushProtoParser, jsonParser, rawStringParser, tempoPushParser, tempoPushNDJSONParser,
  yamlParser, prometheusPushProtoParser, combinedParser, otlpPushProtoParser
} = require('./parsers');

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
      this.bulk.add([[fp,
        [['label', 'RAM usage'], ['profiler_id', process.env.PROFILE]],
        BigInt(Date.now()) * BigInt(1000000),
        process.memoryUsage().rss / 1024 / 1024, ''
      ]])
    }, 1000)
  }
})().catch((err) => {
  logger.error(err, 'Error starting qryn')
  process.exit(1)
})

/* Fastify Helper */
let fastify = require('fastify')({
  logger,
  requestTimeout: parseInt(process.env.FASTIFY_REQUESTTIMEOUT) || 0,
  maxRequestsPerSocket: parseInt(process.env.FASTIFY_MAXREQUESTS) || 0
})

fastify.register(require('fastify-url-data'))
fastify.register(require('@fastify/websocket'))

/* Formbody parser for Prometheus Checks */
fastify.register(require('@fastify/formbody'), { options: { prefix: '/api/v1/'} })

/* Fastify local metrics exporter */
if (process.env.FASTIFY_METRICS) {
  const metricsPlugin = require('fastify-metrics')
  fastify.register(metricsPlugin, { endpoint: '/metrics' })
}
/* CORS Helper */
const CORS = process.env.CORS_ALLOW_ORIGIN || '*'
fastify.register(require('@fastify/cors'), {
  origin: CORS
})

fastify.after((err) => {
  if (err) {
    logger.error({ err }, 'Error creating http response')
    throw err
  }
})

fastify.__post = fastify.post
fastify.post = (route, handler, _parsers) => {
  if (_parsers) {
    for (const t of Object.keys(_parsers)) {
      parsers.register('post', route, t, _parsers[t])
    }
  }
  return fastify.__post(route, handler)
}

fastify.__put = fastify.put
fastify.put = (route, handler) => {
  if (handler.parsers) {
    for (const t of Object.keys(handler.parsers)) {
      parsers.register('put', route, t, handler.parsers[t])
    }
  }
  return fastify.__put(route, handler)
}

fastify.__all = fastify.all
fastify.all = (route, handler) => {
  if (handler.parsers) {
    for (const t of Object.keys(handler.parsers)) {
      parsers.register('post', route, t, handler.parsers[t])
      parsers.register('put', route, t, handler.parsers[t])
    }
  }
  return fastify.__all(route, handler)
}

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
    validate,
    authenticate: true
  })
  fastify.after(() => {
    fastify.addHook('preHandler', fastify.basicAuth)
  })
}

/* 404 Handler */
const handler404 = require('./lib/handlers/404.js').bind(this)
fastify.setNotFoundHandler(handler404)
fastify.setErrorHandler(require('./lib/handlers/errors').handler.bind(this))

/* Hello qryn test API */
const handlerHello = require('./lib/handlers/ready').bind(this)
fastify.get('/hello', handlerHello)
fastify.get('/ready', handlerHello)

/* Write Handler */
const handlerPush = require('./lib/handlers/push.js').bind(this)
fastify.post('/loki/api/v1/push', handlerPush, {
  'application/json': lokiPushJSONParser,
  'application/x-protobuf': lokiPushProtoParser,
  '*': lokiPushJSONParser
})

/* Elastic Write Handler */
const handlerElasticPush = require('./lib/handlers/elastic_index.js').bind(this)
fastify.post('/:target/_doc', handlerElasticPush, {
  'application/json': jsonParser,
  '*': rawStringParser
})
fastify.post('/:target/_create/:id', handlerElasticPush, {
  'application/json': jsonParser,
  '*': rawStringParser
})
fastify.put('/:target/_doc/:id', handlerElasticPush, {
  'application/json': jsonParser,
  '*': rawStringParser
})
fastify.put('/:target/_create/:id', handlerElasticPush, {
  'application/json': jsonParser,
  '*': rawStringParser
})
const handlerElasticBulk = require('./lib/handlers/elastic_bulk.js').bind(this)
fastify.post('/_bulk', handlerElasticBulk, {
  '*': rawStringParser
})
fastify.post('/:target/_bulk', handlerElasticBulk, {
  '*': rawStringParser
})

/* Tempo Write Handler */
this.tempo_tagtrace = process.env.TEMPO_TAGTRACE || false
const handlerTempoPush = require('./lib/handlers/tempo_push.js').bind(this)
fastify.post('/tempo/api/push', handlerTempoPush, {
  'application/json': tempoPushParser,
  'application/x-ndjson': tempoPushNDJSONParser,
  '*': tempoPushParser
})
fastify.post('/api/v2/spans', handlerTempoPush, {
  'application/json': tempoPushParser,
  'application/x-ndjson': tempoPushNDJSONParser,
  '*': tempoPushParser
})

/* Tempo Traces Query Handler */
this.tempo_span = process.env.TEMPO_SPAN || 24
const handlerTempoTraces = require('./lib/handlers/tempo_traces.js').bind(this)
fastify.get('/api/traces/:traceId', handlerTempoTraces)
fastify.get('/api/traces/:traceId/:json', handlerTempoTraces)

/* Tempo Tag Handlers */

const handlerTempoLabel = require('./lib/handlers/tempo_tags').bind(this)
fastify.get('/api/search/tags', handlerTempoLabel)

/* Tempo Tag Value Handler */
const handlerTempoLabelValues = require('./lib/handlers/tempo_values').bind(this)
fastify.get('/api/search/tag/:name/values', handlerTempoLabelValues)

/* Tempo Traces Query Handler */
const handlerTempoSearch = require('./lib/handlers/tempo_search.js').bind(this)
fastify.get('/api/search', handlerTempoSearch)

/* Tempo Echo Handler */
const handlerTempoEcho = require('./lib/handlers/echo.js').bind(this)
fastify.get('/api/echo', handlerTempoEcho)

/* Telegraf HTTP Bulk handler */
const handlerTelegraf = require('./lib/handlers/telegraf.js').bind(this)
fastify.post('/telegraf', handlerTelegraf, {
  '*': jsonParser
})

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

/* Series Handler - experimental support for both Loki and Prometheus */
const handlerSeries = require('./lib/handlers/series.js').bind(this)
fastify.get('/loki/api/v1/series', handlerSeries)
const handlerPromSeries = require('./lib/handlers/prom_series.js').bind(this)
fastify.get('/api/v1/series', handlerPromSeries)

fastify.register(async (fastify) => {
  fastify.get('/loki/api/v1/tail', { websocket: true }, require('./lib/handlers/tail').bind(this))
})

/* ALERT MANAGER Handlers    */
fastify.get('/api/prom/rules', require('./lib/handlers/alerts/get_rules').bind(this))
fastify.get('/api/prom/rules/:ns/:group', require('./lib/handlers/alerts/get_group').bind(this))
fastify.post('/api/prom/rules/:ns', require('./lib/handlers/alerts/post_group').bind(this), {
  '*': yamlParser
})
fastify.delete('/api/prom/rules/:ns/:group', require('./lib/handlers/alerts/del_group').bind(this))
fastify.delete('/api/prom/rules/:ns', require('./lib/handlers/alerts/del_ns').bind(this))
fastify.get('/prometheus/api/v1/rules', require('./lib/handlers/alerts/prom_get_rules').bind(this))

/* PROMETHEUS REMOTE WRITE Handlers */
const promWriteHandler = require('./lib/handlers/prom_push.js').bind(this)
fastify.post('/api/v1/prom/remote/write', promWriteHandler, {
  'application/x-protobuf': prometheusPushProtoParser,
  'application/json': jsonParser,
  '*': combinedParser(prometheusPushProtoParser, jsonParser)
})
fastify.post('/api/prom/remote/write', promWriteHandler, {
  'application/x-protobuf': prometheusPushProtoParser,
  'application/json': jsonParser,
  '*': combinedParser(prometheusPushProtoParser, jsonParser)
})
fastify.post('/prom/remote/write', promWriteHandler, {
  'application/x-protobuf': prometheusPushProtoParser,
  'application/json': jsonParser,
  '*': combinedParser(prometheusPushProtoParser, jsonParser)
})

/* PROMQETHEUS API EMULATION */
const handlerPromQueryRange = require('./lib/handlers/prom_query_range.js').bind(this)
fastify.all('/api/v1/query_range', handlerPromQueryRange)
const handlerPromQuery = require('./lib/handlers/prom_query.js').bind(this)
fastify.all('/api/v1/query', handlerPromQuery)
const handlerPromLabel = require('./lib/handlers/promlabel.js').bind(this)
const handlerPromLabelValues = require('./lib/handlers/promlabel_values.js').bind(this)
fastify.get('/api/v1/labels', handlerPromLabel) // piggyback on qryn labels
fastify.get('/api/v1/label/:name/values', handlerPromLabelValues) // piggyback on qryn values
fastify.post('/api/v1/labels', handlerPromLabel, {
  '*': rawStringParser
}) // piggyback on qryn labels
fastify.post('/api/v1/label/:name/values', handlerPromLabelValues, {
  '*': rawStringParser
}) // piggyback on qryn values
const handlerPromDefault = require('./lib/handlers/prom_default.js').bind(this)
fastify.get('/api/v1/metadata', handlerPromDefault) // default handler TBD
fastify.get('/api/v1/rules', handlerPromDefault) // default handler TBD
fastify.get('/api/v1/query_exemplars', handlerPromDefault) // default handler TBD
fastify.get('/api/v1/status/buildinfo', handlerPromDefault) // default handler TBD

/* NewRelic Log Handler */
const handlerNewrelicLogPush = require('./lib/handlers/newrelic_log_push.js').bind(this)
fastify.post('/log/v1', handlerNewrelicLogPush, {
  'text/plain': jsonParser,
  '*': jsonParser
})

/* INFLUX WRITE Handlers */
const handlerInfluxWrite = require('./lib/handlers/influx_write.js').bind(this)
fastify.post('/write', handlerInfluxWrite, {
  '*': rawStringParser
})
fastify.post('/influx/api/v2/write', handlerInfluxWrite, {
  '*': rawStringParser
})
/* INFLUX HEALTH Handlers */
const handlerInfluxHealth = require('./lib/handlers/influx_health.js').bind(this)
fastify.get('/health', handlerInfluxHealth)
fastify.get('/influx/health', handlerInfluxHealth)

const handlerOTLPPush = require('./lib/handlers/otlp_push').bind(this)
fastify.post('/v1/traces', handlerOTLPPush, {
  '*': otlpPushProtoParser
})

fastify = parsers.init(fastify)

/* QRYN-VIEW Optional Handler */
if (fs.existsSync(path.join(__dirname, 'view/index.html'))) {
  fastify.register(require('@fastify/static'), {
    root: path.join(__dirname, 'view'),
    prefix: '/'
  })
}

// Run API Service
fastify.listen(
  { port: process.env.PORT || 3100,
    host: process.env.HOST || '0.0.0.0'
  },
  (err, address) => {
    if (err) throw err
    logger.info('Qryn API up')
    fastify.log.info(`Qryn API listening on ${address}`)
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
