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
    h: 60000 * 60
  }
  for (const k of Object.keys(durations)) {
    const m = durationStr.match(new RegExp(`^([0-9][.0-9]*)${k}$`))
    if (m) {
      return parseInt(m[1]) * durations[k]
    }
  }
  throw new Error('Unsupported duration')
}
