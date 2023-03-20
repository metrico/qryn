const crypto = require('crypto')
const { flatOTLPAttrs, OTLPgetServiceNames } = require('../utils')
module.exports = class {
  constructor (obj) {
    if (obj.parentId) {
      obj.parentId = this.getId(obj.parentSpanId, 16, false)
    }
    const { local: serviceName, remote: remoteServiceName } = OTLPgetServiceNames(flatOTLPAttrs(obj.attributes))

    this.span_id = this.getId(obj.spanId, 16, true)
    this.trace_id = this.getId(obj.traceId, 32, true)
    this.parent_id = this.getId(obj.parentSpanId, 16, false)
    this.name = obj.name || ''
    this.timestamp_ns = BigInt(obj.startTimeUnixNano)
    this.duration_ns = BigInt(obj.endTimeUnixNano) - this.timestamp_ns
    this.service_name = serviceName
    this.payload_type = 2
    this.payload = JSON.stringify(obj)
    this.tags = {}
    this.tags.name = this.name
    for (const tag of obj.attributes || []) {
      let val = ''
      if (!tag.value) {
        continue
      }
      val = ((tag) => {
        for (const valueKey of ['stringValue', 'boolValue', 'intValue', 'doubleValue']) {
          if (typeof tag.value[valueKey] !== 'undefined') {
            return `${tag.value[valueKey]}`
          }
        }
        return undefined
      })(tag)
      val = val || JSON.stringify(tag.value)
      this.tags[tag.key] = val
    }
    this.tags['service.name'] = serviceName
    this.tags['remoteService.name'] = remoteServiceName
    if (obj.status) {
      this.tags['otel.status_code'] = obj.status.code.toString()
      this.tags['otel.status_description'] = obj.status.message
    }

    this.tags = Object.entries(this.tags)
  }

  /**
   * @returns {string}
   */
  toJson () {
    return JSON.stringify(this, (k, val) => typeof val === 'bigint' ? val.toString() : val)
  }

  /**
   *
   * @param strId {string}
   * @param size {number}
   * @param defaultRandom {boolean}
   * @returns {string}
   */
  getId (strId, size, defaultRandom) {
    if (!strId) {
      return undefined
    }
    strId = Buffer.from(strId, 'base64').toString('hex')
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
