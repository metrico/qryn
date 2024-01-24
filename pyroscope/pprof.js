const messages = require('./profile_pb')

/**
 *
 * @param buf {Uint8Array}
 * @returns {*}
 * @constructor
 */
const readULeb32 = (buf, start) => {
  let res = 0
  let i = start
  for (; (buf[i] & 0x80) === 0x80; i++) {
    res |= (buf[i] & 0x7f) << ((i - start) * 7)
  }
  res |= (buf[i] & 0x7f) << ((i - start) * 7)
  return [res, i - start + 1]
}

class TreeNode {
  constructor (nameIdx, total, self, children) {
    this.nameIdx = nameIdx || 0
    this.prepend = BigInt(0)
    this.total = total || BigInt(0)
    this.self = self || BigInt(0)
    this.children = children || []
  }
}

class Tree {
  constructor () {
    this.names = ['total']
    this.namesMap = { total: 0 }
    this.root = new TreeNode()
    this.sampleType = []
    this.maxSelf = BigInt(0)
  }

  /**
   *
   * @param {Profile} prof
   */
  merge (prof) {
    const functions = prof.getFunctionList().reduce((a, b) => {
      a[b.getId()] = prof.getStringTableList()[b.getName()]
      return a
    }, {})

    const locations = prof.getLocationList().reduce((a, b) => {
      a[b.getId()] = b
      return a
    }, {})
    const getFnName = (l) => functions[l.getLineList()[0].getFunctionId()]

    const valueIdx = prof.getSampleTypeList().findIndex((type) =>
      this.sampleType === `${prof.getStringTableList()[type.getType()]}:${prof.getStringTableList()[type.getUnit()]}`
    )

    for (const l of prof.getLocationList()) {
      const line = getFnName(l)
      if (this.namesMap[line]) {
        continue
      }
      this.names.push(line)
      this.namesMap[line] = this.names.length - 1
    }
    for (const s of prof.getSampleList()) {
      let node = this.root
      for (let i = s.getLocationIdList().length - 1; i >= 0; i--) {
        const location = locations[s.getLocationIdList()[i]]
        const nameIdx = this.namesMap[getFnName(location)]
        let nodeIdx = node.children.findIndex(c => c.nameIdx === nameIdx)
        if (nodeIdx === -1) {
          node.children.push(new TreeNode(nameIdx))
          nodeIdx = node.children.length - 1
        }
        node = node.children[nodeIdx]
        node.total += BigInt(s.getValueList()[valueIdx])
        if (i === 0) {
          node.self += BigInt(s.getValueList()[valueIdx])
          if (node.self > this.maxSelf) {
            this.maxSelf = node.self
          }
        }
      }
    }
    this.root.total = this.root.children.reduce((a, b) => a + b.total, BigInt(0))
  }
}

/**
 *
 * @param t {Tree}
 * @returns {BigInt[][]}
 */
const bfs = (t) => {
  let refs = [t.root]
  let validRefs = true
  let prepend = BigInt(0)
  const putPrepend = (v) => {
    prepend += v
  }
  const getPrepend = () => {
    const res = prepend
    prepend = BigInt(0)
    return res
  }
  const res = [[0, parseInt(t.root.total), parseInt(t.root.self), t.root.nameIdx]]
  for (;validRefs;) {
    validRefs = false
    getPrepend()
    const _res = []
    const _refs = []
    for (const r of refs) {
      putPrepend(r.prepend)
      for (const c of r.children) {
        validRefs = true
        c.prepend = getPrepend()
        _res.push(parseInt(c.prepend), parseInt(c.total), parseInt(c.self), c.nameIdx)
      }
      _refs.push.apply(_refs, r.children)
      if (r.children.length === 0) {
        putPrepend(r.total)
      } else {
        putPrepend(r.self)
      }
    }
    res.push(_res)
    refs = _refs
  }
  return res
}

/**
 *
 * @param {Uint8Array[]} pprofBinaries
 * @param {string} sampleType
 */
const createFlameGraph = (pprofBinaries, sampleType) => {
  console.log(`got ${pprofBinaries.length} profiles`)
  const tree = new Tree()
  tree.sampleType = sampleType
  let start = Date.now()
  for (const p of pprofBinaries) {
    const prof = messages.Profile.deserializeBinary(p)
    tree.merge(prof)
  }
  console.log(`ds + merge took ${Date.now() - start} ms`)
  start = Date.now()
  const levels = bfs(tree)
  console.log(`bfs took ${Date.now() - start} ms`)
  return { levels: levels, names: tree.names, total: parseInt(tree.root.total), maxSelf: parseInt(tree.maxSelf) }
}

module.exports = { createFlameGraph, readULeb32 }
