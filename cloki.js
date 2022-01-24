#!/usr/bin/env node

/*
 * Loki API to Clickhouse Gateway
 * (C) 2018-2021 QXIP BV
 */

this.debug = process.env.DEBUG || false
// const debug = this.debug

this.readonly = process.env.READONLY || false
this.http_user = process.env.CLOKI_LOGIN || false
this.http_password = process.env.CLOKI_PASSWORD || false

require('./plugins/engine')

const DATABASE = require('./lib/db/clickhouse')
const UTILS = require('./lib/utils')

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
const yargs = require('yargs/yargs')
const CLokiClient = require('./lib/db/clickhouse').client
const { getClient } = require('./lib/multitenancy')
const initMultitenancy = require('./lib/multitenancy').init
let fastify = null

async function start () {
  /* Fingerprinting */
  this.fingerPrint = UTILS.fingerPrint
  this.toJSON = UTILS.toJSON

  /* Database this.bulk Helpers */
  this.bulk = DATABASE.cache.bulk // samples
  this.bulk_labels = DATABASE.cache.bulk_labels // labels
  this.labels = DATABASE.cache.labels // in-memory labels

  /* Function Helpers */
  this.labelParser = UTILS.labelParser

  this.reloadFingerprints = DATABASE.reloadFingerprints
  this.scanFingerprints = DATABASE.scanFingerprints
  this.instantQueryScan = DATABASE.instantQueryScan
  this.tempoQueryScan = DATABASE.tempoQueryScan
  this.scanMetricFingerprints = DATABASE.scanMetricFingerprints
  this.tempoQueryScan = DATABASE.tempoQueryScan
  this.scanClickhouse = DATABASE.scanClickhouse

  if (!this.readonly) {
    await upgradeAndRotate()
    await startAlerting()
  }
  initMultitenancy()

  /* Fastify Helper */
  fastify = require('fastify')({
    logger: false,
    bodyLimit: parseInt(process.env.FASTIFY_BODYLIMIT) || 5242880,
    requestTimeout: parseInt(process.env.FASTIFY_REQUESTTIMEOUT) || 0,
    maxRequestsPerSocket: parseInt(process.env.FASTIFY_MAXREQUESTS) || 0
  })
  fastify = fastify.decorateRequest('client', function () {
    return getClient(this.headers ? this.headers['x-scope-orgid'] : undefined)
  })

  fastify.register(require('fastify-url-data'))
  fastify.register(require('fastify-websocket'))

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

  fastify.addContentTypeParser('text/plain', {
    parseAs: 'string'
  }, function (req, body, done) {
    try {
      const json = JSON.parse(body)
      done(null, json)
    } catch (err) {
      err.statusCode = 400
      done(err, undefined)
    }
  })

  fastify.addContentTypeParser('application/yaml', {
    parseAs: 'string'
  }, function (req, body, done) {
    try {
      const json = yaml.parse(body)
      done(null, json)
    } catch (err) {
      err.statusCode = 400
      done(err, undefined)
    }
  })

  try {
    const snappy = require('snappyjs')
    /* Protobuf Handler */
    fastify.addContentTypeParser('application/x-protobuf', { parseAs: 'buffer' },
      async function (req, body, done) {
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
          _data = messages.PushRequest.decode(_data)
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
      })
  } catch (e) {
    console.log(e)
    console.log('Protobuf ingesting is unsupported')
  }

  /* Null content-type handler for CH-MV HTTP PUSH */
  fastify.addContentTypeParser('*', {
    parseAs: 'string'
  }, function (req, body, done) {
    try {
      const json = JSON.parse(body)
      done(null, json)
    } catch (err) {
      err.statusCode = 400
      done(err, undefined)
    }
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

  // ALERT MANAGER

  fastify.get('/api/prom/rules', require('./lib/handlers/alerts/get_rules').bind(this))
  fastify.get('/api/prom/rules/:ns/:group', require('./lib/handlers/alerts/get_group').bind(this))
  fastify.post('/api/prom/rules/:ns', require('./lib/handlers/alerts/post_group').bind(this))
  fastify.delete('/api/prom/rules/:ns/:group', require('./lib/handlers/alerts/del_group').bind(this))
  fastify.delete('/api/prom/rules/:ns', require('./lib/handlers/alerts/del_ns').bind(this))
  fastify.get('/prometheus/api/v1/rules', require('./lib/handlers/alerts/prom_get_rules').bind(this))

  // PROMETHEUS REMOTE WRITE
  fastify.post('/api/v1/prom/remote/write', require('./lib/handlers/prom_push.js').bind(this))
  fastify.post('/api/prom/remote/write', require('./lib/handlers/prom_push.js').bind(this))

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
}

module.exports.start = start

module.exports.stop = () => {
  fastify.close()
  DATABASE.stop()
  require('./parser/transpiler').stop()
  stop()
  require('./lib/multitenancy').stop()
}

const { upgrade, rotate } = require('./lib/db/maintain')

async function upgradeAndRotate () {
  const client = new CLokiClient()
  await upgrade([client])
  const dbs = await client.getAllDBs()
  await upgrade(dbs.map(db => new CLokiClient(db.url)))
  await rotate([{
    client: client,
    samples_days: process.env.SAMPLES_DAYS || 7,
    time_series_days: process.env.LABELS_DAYS || 7,
    storage_policy: process.env.STORAGE_POLICY || ''
  }])
  await rotate(dbs.map(db => ({
    client: new CLokiClient(db.url),
    time_series_days: db.time_series_days || 7,
    samples_days: db.samples_days || 7,
    storage_policy: db.storage_policy || ''
  })))
}

/**
 *
 * @param org {string}
 * @param url {string}
 * @param db {string}
 * @param rotateSamples {number}
 * @param rotateTS {number}
 * @param storagePolicy {string}
 * @returns {Promise<void>}
 */
const addTenant = async (org, url, db, rotateSamples, rotateTS, storagePolicy) => {
  const mainClient = await getClient()
  await mainClient.addTenant(org, url, db, rotateSamples, rotateTS)
  const tenantClient = await getClient(org)
  await upgrade([tenantClient])
  await rotate([{
    client: tenantClient,
    time_series_days: rotateTS,
    samples_days: rotateSamples,
    storage_policy: storagePolicy || ''
  }])
}

// eslint-disable-next-line no-unused-expressions
yargs(process.argv.slice(2)).help().command('update-only', 'update and rotate schema', {},
  (args) => {
    upgradeAndRotate().then(() => process.exit(0), (err) => {
      console.log(err)
      process.exit(1)
    })
  }
).command('rotate-only', 'rotate log tables', {},
  (args) => {
    const { rotate } = require('./lib/db/maintain');
    (async function () {
      const client = new CLokiClient()
      const dbs = await client.getAllDBs()
      await rotate([{
        client: client,
        samples_days: process.env.SAMPLES_DAYS || 7,
        time_series_days: process.env.LABELS_DAYS || 7,
        storage_policy: process.env.STORAGE_POLICY || ''
      }])
      await rotate(dbs.map(db => ({
        client: new CLokiClient(db.url),
        time_series_days: db.time_series_days || 7,
        samples_days: db.samples_days || 7,
        storage_policy: db.storage_policy || ''
      })))
    })().then(() => process.exit(0), (err) => {
      console.log(err)
      process.exit(1)
    })
  }
).command('add-tenant', 'Add an orgid', {}, async (args) => {
  try {
    for (const arg of ['url', 'db', 'org']) {
      if (!args[arg]) {
        throw new Error(`--${arg} argument is required`)
      }
    }
    for (const arg of ['rotate-samples-days', 'rotate-time-series-days']) {
      if (args[arg] && isNaN(parseInt(args[arg]))) {
        throw new Error(`--${arg} argument is not a number`)
      }
    }
    const rotateSamples = parseInt(args.rotateSamplesDays || 7)
    const rotateTS = parseInt(args.rotateTimeSeriesDays || 7)
    await addTenant(args.org, args.url, args.db, rotateSamples, rotateTS, args.storagePolicy || '')
    process.exit(0)
  } catch (e) {
    console.log(e)
    process.exit(1)
  }
}).command('*', 'Serve', {}, (args) => {
  module.exports.start().catch((err) => {
    console.log(err)
    process.exit(1)
  })
}).demandCommand().wrap(72).argv

module.exports.addTenant = addTenant
