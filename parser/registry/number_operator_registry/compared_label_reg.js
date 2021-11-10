const { hasExtraLabels, _and } = require('../common')
/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param index {string}
 * @returns {registry_types.Request}
 */
const genericReq = (token, query, index) => {
  if (token.Child('number_value').Child('duration_value') ||
        token.Child('number_value').Child('bytes_value')) {
    throw new Error('Not implemented')
  }
  const label = token.Child('label').value
  const val = parseInt(token.Child('number_value').value)
  if (isNaN(val)) {
    throw new Error(token.Child('number_value').value + 'is not a number')
  }
  if (query.stream && query.stream.length) {
    return {
      ...query,
      stream: [...(query.stream || []),
        /**
                 *
                 * @param s {DataStream}
                 */
        (s) => s.filter(module.exports.streamWhere[index](label, val))
      ]

    }
  }
  if (hasExtraLabels(query)) {
    return _and(query, [module.exports.extraLabelsWhere[index](label, val)])
  }
  return _and(query, module.exports.simpleWhere[index](label, val))
}

/**
 *
 * @param label {string}
 * @param val {string}
 * @param sign {string}
 * @returns {[string]}
 */
const genericSimpleLabelSearch =
    (label, val, sign) => [
      'and',
      `JSONHas(labels, '${label}')`,
      `toFloat64OrNull(JSONExtractString(labels, '${label}')) ${sign} ${val}`]

/**
 *
 * @param lbl {string}
 * @param val {string}
 * @param sign {string}
 * @returns {[string]}
 */
const genericExtraLabelSearch =
    (lbl, val, sign) => ['or',
        `arrayExists(x -> x.1 == '${lbl}' AND (coalesce(toFloat64OrNull(x.2) ${sign} ${val}, 0)), extra_labels) != 0`,
        [
          'AND',
            `arrayExists(x -> x.1 == '${lbl}', extra_labels) == 0`,
            ...(genericSimpleLabelSearch(lbl, val, sign).slice(1))
        ]
    ]

const genericStreamSearch = (label, fn) =>
  (e) => {
    if (e.EOF) {
      return true
    }
    if (!e || !e.labels || !e.labels[label]) {
      return false
    }
    const val = parseFloat(e.labels[label])
    if (isNaN(val)) {
      return false
    }
    return fn(val)
  }

module.exports.simpleWhere = {
  eq: (label, val) => genericSimpleLabelSearch(label, val, '=='),
  neq: (label, val) => genericSimpleLabelSearch(label, val, '!='),
  ge: (label, val) => genericSimpleLabelSearch(label, val, '>='),
  gt: (label, val) => genericSimpleLabelSearch(label, val, '>'),
  le: (label, val) => genericSimpleLabelSearch(label, val, '<='),
  lt: (label, val) => genericSimpleLabelSearch(label, val, '<')
}

module.exports.extraLabelsWhere = {
  eq: (label, val) => genericExtraLabelSearch(label, val, '=='),
  neq: (label, val) => genericExtraLabelSearch(label, val, '!='),
  ge: (label, val) => genericExtraLabelSearch(label, val, '>='),
  gt: (label, val) => genericExtraLabelSearch(label, val, '>'),
  le: (label, val) => genericExtraLabelSearch(label, val, '<='),
  lt: (label, val) => genericExtraLabelSearch(label, val, '<')
}

module.exports.streamWhere = {
  eq: (label, val) => genericStreamSearch(label, (_val) => Math.abs(val - _val) < 1e-10),
  neq: (label, val) => genericStreamSearch(label, (_val) => Math.abs(val - _val) > 1e-10),
  ge: (label, val) => genericStreamSearch(label, (_val) => _val >= val),
  gt: (label, val) => genericStreamSearch(label, (_val) => _val > val),
  le: (label, val) => genericStreamSearch(label, (_val) => _val <= val),
  lt: (label, val) => genericStreamSearch(label, (_val) => _val < val)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq = (token, query) => {
  return genericReq(token, query, 'eq')
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.neq = (token, query) => {
  return genericReq(token, query, 'neq')
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.gt = (token, query) => {
  return genericReq(token, query, 'gt')
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.ge = (token, query) => {
  return genericReq(token, query, 'ge')
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.lt = (token, query) => {
  return genericReq(token, query, 'lt')
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.le = (token, query) => {
  return genericReq(token, query, 'le')
}
