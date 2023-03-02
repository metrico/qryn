require('./wasm_exec')
const fs = require('fs')
let wasm


const init = async () => {
    if (wasm) {
        return
    }
    const go = new Go();
    const obj = await WebAssembly.instantiate(fs.readFileSync(__dirname + '/wasm.wasm'), go.importObject)
    wasm = obj.instance
}

const parse = (str) => {
    const buff = (new TextEncoder()).encode(str)
    const inAddr = wasm.exports.CreateBuff(buff.length)
    let mem = new Uint8Array(wasm.exports.memory.buffer)
    let inArr = mem.subarray(inAddr, inAddr + buff.length)
    inArr.set(buff, 0)
    wasm.exports.ParseBytes()
    const outAddr = wasm.exports.GetResp()
    mem = new Uint8Array(wasm.exports.memory.buffer)
    let outArr = mem.subarray(outAddr, outAddr + wasm.exports.GetLen())
    let resp = (new TextDecoder()).decode(outArr)
    wasm.exports.Free()
    resp = JSON.parse(resp)
    if (resp.error) {
        throw new Error(resp.error)
    }
    return resp
}

module.exports = {
    parse,
    init
}
