/**
 *
 * @param labels {Object | string[] | string}
 * @returns {Object}
 */
module.exports.parseLabels = (labels) => {
  if (Array.isArray(labels)) {
    return labels.reduce((sum, l) => {
      sum[l[0]] = l[1]
      return sum
    }, {})
  }
  if (typeof labels === 'object') {
    return labels
  }
  return JSON.parse(labels)
}

/**
 *
 * @param labels {Object | string[] | string}
 * @returns {string}
 */
module.exports.hashLabels = (labels) => {
  if (Array.isArray(labels)) {
    return JSON.stringify(labels)
  }
  if (typeof labels === 'object' && labels !== null) {
    const res = [...Object.entries(labels)]
    res.sort()
    return JSON.stringify(labels)
  }
  return labels
}

/**
 *
 * @param durationStr {string}
 * @returns {number}
 */
module.exports.durationToMs = (durationStr) => {
  const durations = {
    ns: 1 / 1000000,
    us: 1 / 1000,
    ms: 1,
    s: 1000,
    m: 60000,
    h: 60000 * 60,
    d: 60000 * 60 * 24,
    w: 60000 * 60 * 24 * 7
  }
  for (const k of Object.keys(durations)) {
    const m = durationStr.match(new RegExp(`^([0-9][.0-9]*)${k}$`))
    if (m) {
      return parseInt(m[1]) * durations[k]
    }
  }
  throw new Error('Unsupported duration')
}

/**
 *
 * @param durationStr {string}
 * @returns {number}
 */
module.exports.durationToNs = (durationStr) => {
  const durations = {
    ns: 1,
    us: 1000,
    ms: 1000000,
    s: 1000000000,
    m: 60000000000,
    h: 60000000000 * 60,
    d: 60000000000 * 60 * 24,
    w: 60000000000 * 60 * 24 * 7
  }
  for (const k of Object.keys(durations)) {
    const m = durationStr.match(new RegExp(`^([0-9][.0-9]*)${k}$`))
    if (m) {
      return parseInt(m[1]) * durations[k]
    }
  }
  throw new Error('Unsupported duration')
}

module.exports.asyncLogError = async (err, logger) => {
  try {
    const resp = err.response || err.err.response
    if (resp) {
      if (typeof resp.data === 'object') {
        err.responseData = ''
        err.response.data.on('data', data => { err.responseData += data })
        await new Promise((resolve) => err.response.data.once('end', resolve))
      } else {
        err.responseData = err.response.data
      }
      logger.error(err)
    }
  } catch (e) {
    logger.error(err)
  }
}

module.exports.isOmitTablesCreation = () => process.env.OMIT_CREATE_TABLES === '1'

module.exports.LineFmtOption = () => process.env.LINE_FMT || 'handlebars'

module.exports.errors = require('./lib/handlers/errors')
/**
 * @returns {string}
 */
module.exports.samplesOrderingRule = () => {
  return process.env.ADVANCED_SAMPLES_ORDERING
    ? process.env.ADVANCED_SAMPLES_ORDERING
    : 'timestamp_ns'
}

/**
 * @returns {boolean}
 */
module.exports.isCustomSamplesOrderingRule = () => {
  return process.env.ADVANCED_SAMPLES_ORDERING && process.env.ADVANCED_SAMPLES_ORDERING !== 'timestamp_ns'
}

module.exports.CORS = process.env.CORS_ALLOW_ORIGIN || '*'

module.exports.clusterName = process.env.CLUSTER_NAME
