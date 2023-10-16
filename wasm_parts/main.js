require('./wasm_exec')
const { join } = require('path')
const WASM_URL = join(__dirname, 'main.wasm.gz')
const fs = require('fs')
const { gunzipSync } = require('zlib')

let counter = 0
var go
var wasm

async function init () {
  const _go = new Go()
  const _wasm = await WebAssembly.instantiate(
    gunzipSync(fs.readFileSync(WASM_URL)), _go.importObject)
  _go.run(_wasm.instance)
  wasm = _wasm.instance
  go = _go
}

init()
setInterval(async () => {
  init()
}, 600000)

/**
 *
 * @param query {string}
 * @param startMs {number}
 * @param endMs {number}
 * @param stepMs {number}
 * @param getData {function}
 * @returns {Promise<string>}
 */
module.exports.pqlRangeQuery = async (query, startMs, endMs, stepMs, getData) => {
  const _wasm = wasm
  const start = startMs || Date.now() - 300000
  const end = endMs || Date.now()
  const step = stepMs || 15000
  return await pql(query,
    (ctx) => _wasm.exports.pqlRangeQuery(ctx.id, start, end, step),
    (matchers) => getData(matchers, start, end))
}

/**
 *
 * @param query {string}
 * @param timeMs {number}
 * @param getData {function}
 * @returns {Promise<string>}
 */
module.exports.pqlInstantQuery = async (query, timeMs, getData) => {
  const time = timeMs || Date.now()
  const _wasm = wasm
  return await pql(query,
    (ctx) => _wasm.exports.pqlInstantQuery(ctx.id, time),
    (matchers) => getData(matchers, time - 300000, time))
}

module.exports.pqlMatchers = (query) => {
  const _wasm = wasm
  const id = counter
  counter = (counter + 1) & 0xFFFFFFFF
  const ctx = new Ctx(id, _wasm)
  ctx.create()
  try {
      ctx.write(query)
      const res1 = _wasm.exports.pqlSeries(id)
      if (res1 !== 0) {
        throw new Error('pql failed: ', ctx.read())
      }
      /** @type {[[[string]]]} */
      const matchersObj = JSON.parse(ctx.read())
      return matchersObj
  } finally {
      ctx.destroy()
  }
}

/**
 *
 * @param query {string}
 * @param wasmCall {function}
 * @param getData {function}
 * @returns {Promise<string>}
 */
const pql = async (query, wasmCall, getData) => {
  const reqId = counter
  counter = (counter + 1) & 0xFFFFFFFF
  const _wasm = wasm
  const ctx = new Ctx(reqId, _wasm)
  ctx.create()
  try {
    ctx.write(query)
    const res1 = wasmCall(ctx)
    if (res1 !== 0) {
      throw new Error('pql failed: ', ctx.read())
    }

    const matchersObj = JSON.parse(ctx.read())

    const matchersResults = await Promise.all(
      matchersObj.map(async (matchers, i) => {
        const data = await getData(matchers)
        return { matchers, data }
      }))

    const writer = new Uint8ArrayWriter(new Uint8Array(1024))
    for (const { matchers, data } of matchersResults) {
      writer.writeString(JSON.stringify(matchers))
      writer.writeBytes([data])
    }

    ctx.write(writer.buffer())
    _wasm.exports.onDataLoad(reqId)
    return ctx.read()
  } finally {
    ctx.destroy()
  }
}
class Ctx {
  constructor (id, wasm) {
    this.wasm = wasm
    this.id = id
  }

  create () {
    this.wasm.exports.createCtx(this.id)
  }

  destroy () {
    this.wasm.exports.dealloc(this.id)
  }

  /**
   *
   * @param data {Uint8Array | string}
   */
  write (data) {
    if (typeof data === 'string') {
      data = (new TextEncoder()).encode(data)
    }
    this.wasm.exports.alloc(this.id, data.length)
    const ptr = wasm.exports.alloc(this.id, data.length)
    new Uint8Array(wasm.exports.memory.buffer).set(data, ptr)
  }

  /**
   * @returns {String}
   */
  read() {
    const [resPtr, resLen] = [
      this.wasm.exports.getCtxResponse(this.id),
      this.wasm.exports.getCtxResponseLen(this.id)
    ]
    return new TextDecoder().decode(new Uint8Array(wasm.exports.memory.buffer).subarray(resPtr, resPtr + resLen))
  }
}

class Uint8ArrayWriter {
  /**
   *
   * @param buf {Uint8Array}
   */
  constructor (buf) {
    this.buf = buf
    this.i = 0
  }

  maybeGrow (len) {
    for (;this.i + len > this.buf.length;) {
      const _buf = new Uint8Array(this.buf.length + 1024 * 1024)
      _buf.set(this.buf)
      this.buf = _buf
    }
  }

  /**
   *
   * @param n {number}
   */
  writeULeb (n) {
    this.maybeGrow(9)
    let _n = n
    if (n === 0) {
      this.buf[this.i] = 0
      this.i++
      return
    }
    while (_n > 0) {
      let part = _n & 0x7f
      _n >>= 7
      if (_n > 0) {
        part |= 0x80
      }
      this.buf[this.i] = part
      this.i++
    }
  }

  /**
   *
   * @param str {string}
   */
  writeString (str) {
    const bStr = (new TextEncoder()).encode(str)
    this.writeULeb(bStr.length)
    this.buf.set(bStr, this.i)
    this.i += bStr.length
    return this
  }

  /**
   *
   * @param buf {Uint8Array[]}
   */
  writeBytes (buf) {
    for (const b of buf) {
      this.writeULeb(b.length)
      this.maybeGrow(b.length)
      this.buf.set(b, this.i)
      this.i += b.length
    }
    return this
  }

  buffer () {
    return this.buf.subarray(0, this.i)
  }
}
