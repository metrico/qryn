const { EventEmitter } = require('events')
const { QrynError } = require('./lib/handlers/errors')
const StreamArray = require('stream-json/streamers/StreamArray')
const { parser: jsonlParser } = require('stream-json/jsonl/Parser')
const yaml = require('yaml')
let snappy = null
try {
  snappy = require('snappyjs')
} catch (e) {}
const gzip = require('node-gzip')
const protobufjs = require('protobufjs')
const path = require('path')
const WriteRequest = protobufjs.loadSync(path.join(__dirname, 'lib', 'prompb.proto')).lookupType('WriteRequest')
const PushRequest = protobufjs.loadSync(path.join(__dirname, 'lib', 'loki.proto')).lookupType('PushRequest')
const OTLPTraceData = protobufjs.loadSync(path.join(__dirname, 'lib', 'otlp.proto')).lookupType('TracesData')
/**
 *
 * @param req {FastifyRequest}
 */
const lokiPushJSONParser = async (req) => {
  try {
    const length = getContentLength(req, 1e9)
    if (length > 5e6) {
      return
    }
    await shaper.register(length)
    return JSON.parse(await getContentBody(req))
  } catch (err) {
    err.statusCode = 400
    throw err
  }
}

/**
 *
 * @param req {FastifyRequest}
 * @returns {any}
 */
async function tempoPushNDJSONParser (req) {
  const parser = req.raw.pipe(jsonlParser())
  parser.on('error', err => { parser.error = err })
  return parser
}

/**
 * @param req {FastifyRequest}
 * @returns {any}
 */
async function jsonParser (req) {
  return JSON.parse(await getContentBody(req))
}

/**
 * @param req {FastifyRequest}
 * @returns {any}
 */
async function yamlParser (req) {
  return yaml.parse(await getContentBody(req))
}

/**
 *
 * @param req {FastifyRequest}
 * @returns {any}
 */
async function tempoPushParser (req) {
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
 */
async function rawStringParser (req) {
  return await getContentBody(req)
}

/**
 * @param req {FastifyRequest}
 */
async function lokiPushProtoParser (req) {
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
 */
async function prometheusPushProtoParser (req) {
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
 */
async function otlpPushProtoParser (req) {
  const length = getContentLength(req, 5e6)
  await shaper.register(length)
  let body = []
  req.raw.on('data', (data) => {
    body.push(data)
  })
  await new Promise(resolve => req.raw.once('end', resolve))
  body = Buffer.concat(body)
  try {
    body = await gzip.ungzip(body)
  } catch (e) {}

  body = OTLPTraceData.decode(body)
  return body
}

/**
 *
 * @param req {FastifyRequest}
 * @returns {*}
 */
function tempoNDJsonParser (req) {
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
  return async (req) => {
    for (const p of subparsers) {
      try {
        return await p(req)
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
   * @param req {FastifyRequest}
   */
  parse: (contentType) => async (req) => {
    const find = (obj, path) => {
      for (const p of path) {
        if (!obj[p]) {
          return null
        }
        obj = obj[p]
      }
      return obj
    }
    const parser = find(parsers._parsers, [contentType, req.routerMethod, req.routerPath]) ||
      find(parsers._parsers, ['*', req.routerMethod, req.routerPath])
    if (!parser) {
      throw new Error('undefined parser')
    }
    return await parser(req)
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
 * @returns {Promise<string>}
 */
async function getContentBody (req) {
  if (req._rawBody) {
    return req._rawBody
  }
  let body = ''
  req.raw.on('data', data => {
    body += data.toString()
  })
  await new Promise(resolve => req.raw.once('end', resolve))
  req._rawBody = body
  return body
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
  parsers
}
