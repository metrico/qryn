const fs = require('fs')
const path = require('path')
let inst = null

module.exports.compile = async (format) => {
  if (!inst) {
    require('./wasm_exec')
    const go = new global.Go()
    const wasm = fs.readFileSync(path.join(__dirname, 'go_txttmpl.wasm'))
    const wasmModule = await WebAssembly.instantiate(wasm, go.importObject)
    go.run(wasmModule.instance)
    inst = true
  }
  const res = global.GO_TXTTMPL_NewTemplate(format)
  if (res.err) {
    throw new Error(res.err)
  }
  return {
    process: (labels) => {
      return global.GO_TXTTMPL_ProcessLine(res.id, Object.entries(labels).map(e => `${e[0]}\x01${e[1]}`).join('\x01'))
    },
    done: () => {
      global.GO_TXTTMPL_ReleaseTemplate(res.id)
    }
  }
}

module.exports.stop = () => {
  if (inst) {
    global.GO_TXTTMPL_End()
    inst = null
  }
}
