const crypto = require('crypto')

module.exports = class {
  constructor (obj) {
    this.timestamp_ns = obj.timestamp_ns
    this.type = obj.type
    this.sample_unit = obj.sample_unit
    this.sample_type = obj.sample_type
    this.period_type = obj.period_type
    this.period_unit = obj.period_unit
    this.tags = obj.tags
    this.profile_id = this.randomUUID()
    this.value = obj.value
    this.duration_ns = obj.duration_ns
    this.payload_type = obj.payload_type
    this.payload = obj.payload
  }

  toJson () {
    return JSON.stringify(this)
  }


  randomUUID() {
    return crypto.randomUUID().toString().replace(/-/g, '')
  }
}
