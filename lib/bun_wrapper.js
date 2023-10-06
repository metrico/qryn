const { Transform } = require('stream')
const log = require('./logger')
const { EventEmitter } = require('events')

class BodyStream extends Transform {
  _transform (chunk, encoding, callback) {
    callback(null, chunk)
  }

  once (event, listerer) {
    const self = this
    const _listener = (e) => {
      listerer(e)
      self.removeListener(event, _listener)
    }
    this.on(event, _listener)
  }
}

const wrapper = (handler, parsers) => {
  /**
   * @param ctx {Request}
   */
  const res = async (ctx, server) => {
    let response = ''
    let status = 200
    let reqBody = ''
    let headers = {}

    const stream = new BodyStream()
    setTimeout(async () => {
      if (!ctx.body) {
        stream.end()
        return
      }
      for await (const chunk of ctx.body) {
        stream.write(chunk)
      }
      stream.end()
    })
    const req = {
      headers: Object.fromEntries(ctx.headers.entries()),
      raw: stream,
      log: log,
      params: ctx.params || {},
      query: {}
    }
    for (const [key, value] of (new URL(ctx.url)).searchParams) {
      if (!(key in req.query)) {
        req.query[key] = value
        continue
      }
      req.query[key] = Array.isArray(req.query[key])
        ? [...req.query[key], value]
        : [req.query[key], value]
    }
    const res = {
      send: (msg) => {
        response = msg
      },
      code: (code) => {
        status = code
        return res
      },
      header: (key, value) => {
        headers[key] = value
        return res
      },
      headers: (hdrs) => {
        headers = { ...headers, ...hdrs }
        return res
      }
    }

    if (parsers) {
      const contentType = (ctx.headers.get('Content-Type') || '')
      let ok = false
      for (const [type, parser] of Object.entries(parsers)) {
        if (type !== '*' && contentType.indexOf(type) > -1) {
          log.debug(`parsing ${type}`)
          reqBody = await parser(req, stream)
          ok = true
          log.debug(`parsing ${type} ok`)
        }
      }
      if (!ok && parsers['*']) {
        log.debug('parsing *')
        reqBody = await parsers['*'](req, stream)
        ok = true
        log.debug('parsing * ok')
      }
      if (!ok) {
        throw new Error('undefined content type ' + contentType)
      }
    }

    req.body = reqBody || stream

    let result = handler(req, res)
    if (result && result.then) {
      result = await result
    }
    if (result && result.on) {
      response = ''
      result.on('data', (d) => {
        response += d
      })
      await new Promise((resolve, reject) => {
        result.on('end', resolve)
        result.on('error', reject)
        result.on('close', resolve)
      })
      result = null
    }
    if (result) {
      response = result
    }
    if (response instanceof Object && typeof response !== 'string') {
      response = JSON.stringify(response)
    }
    return new Response(response, { status: status, headers: headers })
  }
  return res
}

const wsWrapper = (handler) => {
  /**
   * @param ctx {Request}
   */
  const res = {
    open: async (ctx, server) => {
      const req = {
        headers: Object.fromEntries(ctx.data.ctx.headers.entries()),
        log: log,
        query: {}
      }
      for (const [key, value] of (new URL(ctx.data.ctx.url)).searchParams) {
        if (!(key in req.query)) {
          req.query[key] = value
          continue
        }
        req.query[key] = Array.isArray(req.query[key])
          ? [...req.query[key], value]
          : [req.query[key], value]
      }

      ctx.closeEmitter = new EventEmitter()
      ctx.closeEmitter.send = ctx.send.bind(ctx)

      const ws = {
        socket: ctx.closeEmitter
      }

      const result = handler(ws, { query: req.query })
      if (result && result.then) {
        await result
      }
    },
    close: (ctx) => { ctx.closeEmitter.emit('close') }
  }
  return res
}

module.exports = {
  wrapper,
  wsWrapper
}
