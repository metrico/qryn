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
