const { EventEmitter } = require('events')
const { QrynError } = require('./lib/handlers/errors')
const StreamArray = require('stream-json/streamers/StreamArray')
const { parser: jsonlParser } = require('stream-json/jsonl/Parser')
const yaml = require('yaml')
let snappy = null
try {
  snappy = require('snappyjs')
} catch (e) {}
const stream = require('stream')
const protobufjs = require('protobufjs')
const path = require('path')
const WriteRequest = protobufjs.loadSync(path.join(__dirname, 'lib', 'prompb.proto')).lookupType('WriteRequest')
const PushRequest = protobufjs.loadSync(path.join(__dirname, 'lib', 'loki.proto')).lookupType('PushRequest')
const OTLPTraceData = protobufjs.loadSync(path.join(__dirname, 'lib', 'otlp.proto')).lookupType('TracesData')
const { parse: queryParser } = require('fast-querystring')

/**
 *
 * @param req {FastifyRequest}
 * @param payload {Stream}
 * @returns {any}
 */
const wwwFormParser = async (req, payload) => {
  return queryParser(await getContentBody(req, payload))
}

/**
 *
 * @param req {FastifyRequest}
 * @param payload {Stream}
 */
const lokiPushJSONParser = async (req, payload) => {
  try {
    const length = getContentLength(req, 1e9)
    if (length > 5 * 1024 * 1024) {
      return
    }
    await shaper.register(length)
    const body = await getContentBody(req, payload)
    return JSON.parse(body)
  } catch (err) {
    err.statusCode = 400
    throw err
  }
}

/**
 *
 * @param req {FastifyRequest}
 * @param payload {Stream}
 * @returns {any}
 */
async function tempoPushNDJSONParser (req, payload) {
  const parser = payload.pipe(jsonlParser())
  parser.on('error', err => { parser.error = err })
  return parser
}

/**
 * @param req {FastifyRequest}
 * @param payload {Stream}
 * @returns {any}
 */
async function jsonParser (req, payload) {
  return JSON.parse(await getContentBody(req, payload))
}

/**
 * @param req {FastifyRequest}
 * @param payload {Stream}
 * @returns {any}
 */
async function yamlParser (req, payload) {
  return yaml.parse(await getContentBody(req, payload))
}

/**
 *
 * @param req {FastifyRequest}
 * @param payload {Stream}
 * @returns {any}
 */
async function tempoPushParser (req, payload) {
  const firstData = await new Promise((resolve, reject) => {
    req.raw.once('data', resolve)
    req.raw.once('error', reject)
    req.raw.once('close', () => resolve(null))
    req.raw.once('end', () => resolve(null))
  })
  const parser = StreamArray.withParser()
  parser.on('error', err => { parser.error = err })
  parser.write(firstData || '[]')
  if (!firstData) {
    parser.end()
    return parser
  }
  req.raw.pipe(parser)
  return parser
}

/**
 * @param req {FastifyRequest}
 * @param payload {Stream}
 */
async function rawStringParser (req, payload) {
  return await getContentBody(req, payload)
}

/**
 * @param req {FastifyRequest}
 * @param payload {Stream}
 */
async function lokiPushProtoParser (req, payload) {
  if (!snappy) {
    throw new Error('snappy not found')
  }
  const length = getContentLength(req, 5e6)
  await shaper.register(length)
  const body = []
  req.raw.on('data', (data) => {
    body.push(data)
  })
  await new Promise(resolve => req.raw.once('end', resolve))
  let _data = await snappy.uncompress(Buffer.concat(body))
  _data = PushRequest.decode(_data)
  _data.streams = _data.streams.map(s => ({
    ...s,
    entries: s.entries.map(e => {
      const ts = e.timestamp
        ? BigInt(e.timestamp.seconds) * BigInt(1e9) + BigInt(e.timestamp.nanos)
        : BigInt(Date.now().toString() + '000000')
      return {
        ...e,
        timestamp: ts
      }
    })
  }))
  return _data.streams
}

/**
 * @param req {FastifyRequest}
 * @param payload {Stream}
 */
async function prometheusPushProtoParser (req, payload) {
  if (!snappy) {
    throw new Error('snappy not found')
  }
  const length = getContentLength(req, 5e6)
  await shaper.register(length)
  const body = []
  req.raw.on('data', (data) => {
    body.push(data)
  })
  await new Promise(resolve => req.raw.once('end', resolve))
  let _data = await snappy.uncompress(Buffer.concat(body))
  _data = WriteRequest.decode(_data)
  _data.timeseries = _data.timeseries.map(s => ({
    ...s,
    samples: s.samples.map(e => {
      const nanos = e.timestamp + '000000'
      return {
        ...e,
        timestamp: nanos
      }
    })
  }))
  return _data
}

/**
 * @param req {FastifyRequest}
 * @param payload {Stream} zlib.Gunzip
 */
async function otlpPushProtoParser (req, payload) {
  const length = getContentLength(req, 5e6)
  await shaper.register(length)
  let body = []
  const otelStream = stream.Readable.from(payload)
  otelStream.on('data', data => {
    body.push(data)
  })
  await new Promise(resolve => otelStream.once('end', resolve))
  body = Buffer.concat(body)
  body = OTLPTraceData.toObject(OTLPTraceData.decode(body), {
    longs: String,
    bytes: String
  })
  return body
}

/**
 *
 * @param req {FastifyRequest}
 * @param payload {Stream}
 * @returns {*}
 */
function tempoNDJsonParser (req, payload) {
  const parser = req.raw.pipe(jsonlParser())
  parser.on('error', err => { parser.error = err })
  return parser
}

/**
 *
 * @param subparsers {function(FastifyRequest): Promise<*|undefined>}
 * @returns {function(FastifyRequest): Promise<*|undefined>}
 */
function combinedParser (...subparsers) {
  /**
   *
   * @param req {FastifyRequest}
   * @returns {any}
   */
  return async (req, payload) => {
    for (const p of subparsers) {
      try {
        return await p(req, payload)
      } catch (e) {}
    }
    return undefined
  }
}

const parsers = {
  _parsers: {},
  /**
   *
   * @param fastify {Fastify}
   */
  init: (fastify) => {
    for (const type of Object.keys(parsers._parsers)) {
      fastify.addContentTypeParser(type, parsers.parse(type))
    }
    return fastify
  },

  /**
   *
   * @param contentType {string}
   * @returns {function(FastifyRequest, Stream): Promise<*>}
   */
  parse: (contentType) =>
    /**
     *
     * @param req {FastifyRequest}
     * @param payload {Stream}
     */
    async (req, payload) => {
      const find = (obj, path) => {
        for (const p of path) {
          if (!obj[p]) {
            return null
          }
          obj = obj[p]
        }
        return obj
      }
      const parser = find(parsers._parsers, [contentType, req.routeOptions.method, req.routeOptions.url]) ||
        find(parsers._parsers, ['*', req.routeOptions.method, req.routeOptions.url])
      if (!parser) {
        throw new Error(`undefined parser for ${contentType} ${req.routeOptions.method} ${req.routeOptions.url}`)
      }
      return await parser(req, payload)
    },

  /**
   *
   * @param method {string}
   * @param route {string}
   * @param contentType {string}
   * @param parser {function(FastifyRequest): Promise<any>}
   */
  register: (method, route, contentType, parser) => {
    parsers._parsers[contentType] = parsers._parsers[contentType] || {}
    parsers._parsers[contentType][method.toUpperCase()] = parsers._parsers[contentType][method.toUpperCase()] || {}
    parsers._parsers[contentType][method.toUpperCase()][route] = parser
  }
}

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
    while (shaper.onParse + size > 50e6) {
      await new Promise(resolve => { shaper.onParsed.once('parsed', resolve) })
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
    return 5 * 1024 * 1024
  }
  const res = parseInt(req.headers['content-length'])
  if (limit && res > limit) {
    throw new QrynError(400, 'Request is too big')
  }
  return res
}

/**
 *
 * @param req {FastifyRequest}
 * @param payload {Stream}
 * @returns {Promise<string>}
 */
async function getContentBody (req, payload) {
  if (req._rawBody) {
    return req._rawBody
  }
  const body = []
  payload.on('data', data => {
    body.push(data)// += data.toString()
  })
  if (payload.isPaused && payload.isPaused()) {
    payload.resume()
  }
  await new Promise(resolve => {
    payload.on('end', resolve)
    payload.on('close', resolve)
  })
  req._rawBody = Buffer.concat(body).toString()
  return Buffer.concat(body).toString()
}

module.exports = {
  getContentBody,
  getContentLength,
  shaper,
  lokiPushJSONParser,
  tempoPushParser,
  tempoPushNDJSONParser,
  jsonParser,
  yamlParser,
  combinedParser,
  rawStringParser,
  lokiPushProtoParser,
  prometheusPushProtoParser,
  tempoNDJsonParser,
  otlpPushProtoParser,
  wwwFormParser,
  parsers
}
