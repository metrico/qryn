const crypto = require('crypto')
module.exports = class {
  constructor (obj) {
    if (obj.parentId) {
      obj.parentId = this.getId(obj.parentId, 16, false)
    }
    this.span_id = this.getId(obj.id, 16, true)
    this.trace_id = this.getId(obj.traceId, 32, true)
    this.parent_id = this.getId(obj.parentId, 16, false)
    this.name = obj.name || ''
    this.timestamp_ns = BigInt(obj.timestamp) * BigInt(1000)
    this.duration_ns = BigInt(obj.duration || 1) * BigInt(1000)
    this.service_name = obj.localEndpoint?.serviceName || obj.remoteEndpoint?.serviceName || ''
    this.payload_type = 1
    this.payload = JSON.stringify(obj)
    this.tags = {}
    this.tags.name = this.name
    this.tags['service.name'] = this.service_name
    for (const tag of Object.entries(obj.tags || {})) {
      this.tags[tag[0]] = tag[1]
    }
    this.tags = Object.entries(this.tags)
  }

  /**
   * @returns {string}
   */
  toJson () {
    const res = {
      ...this,
      timestamp_ns: this.timestamp_ns.toString(),
      duration_ns: this.duration_ns.toString()
    }
    return JSON.stringify(res)
    //return JSON.stringify(this, (k, val) => typeof val === 'bigint' ? val.toString() : val)
  }

  /**
   *
   * @param strId {string}
   * @param size {number}
   * @param defaultRandom {boolean}
   * @returns {string}
   */
  getId (strId, size, defaultRandom) {
    strId = (new Array(size)).fill('0').join('') + strId
    strId = strId.substring(strId.length - size)
    if (strId && strId.match(new RegExp(`^[0-9a-f]{${size}}$`))) {
      return strId
    }
    return defaultRandom
      ? crypto.randomUUID().toString().replace(/-/g, '').substring(0, size)
      : null
  }
}
