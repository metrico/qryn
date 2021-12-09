const { hasExtraLabels, hasStream, addStream } = require('../common')
const Sql = require('clickhouse-sql')
/**
 *
 * @param token {Token}
 * @param query {Select}
 * @param index {string}
 * @returns {Select}
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
  if (hasStream(query)) {
    return addStream(query, (s) => s.filter(module.exports.streamWhere[index](label, val)))
  }
  if (hasExtraLabels(query)) {
    return query.where(module.exports.extraLabelsWhere[index](label, val))
  }
  return query.where(module.exports.simpleWhere[index](label, val))
}

/**
 *
 * @param label {string}
 * @param val {string}
 * @param sign {Function}
 * @returns {Conditions}
 */
const genericSimpleLabelSearch =
    (label, val, sign) => Sql.And(
      Sql.Eq(new Sql.Raw(`JSONHas(labels, '${label}')`), 1),
      sign(new Sql.Raw(`toFloat64OrNull(JSONExtractString(labels, '${label}'))`), val)
    )

/**
 *
 * @param lbl {string}
 * @param val {string}
 * @param sign {Function}
 * @returns {Conditions}
 */
const genericExtraLabelSearch =
    (lbl, val, sign) => Sql.Or(
      Sql.Ne(new Sql.Raw(
        `arrayExists(x -> x.1 == '${lbl}' AND (coalesce(` +
          sign(new Sql.Raw('toFloat64OrNull(x.2)'), val).toString() + ', 0)), extra_labels)'
      ), 0),
      Sql.And(
        Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == '${lbl}', extra_labels)`), 0),
        genericSimpleLabelSearch(lbl, val, sign)
      )
    )

const genericStreamSearch = (label, fn) =>
  (e) => {
    if (!e || !e.labels || !e.labels[label]) {
      return false
    }
    if (e.EOF) {
      return true
    }
    const val = parseFloat(e.labels[label])
    if (isNaN(val)) {
      return false
    }
    return fn(val)
  }

module.exports.simpleWhere = {
  eq: (label, val) => genericSimpleLabelSearch(label, val, Sql.Eq),
  neq: (label, val) => genericSimpleLabelSearch(label, val, Sql.Ne),
  ge: (label, val) => genericSimpleLabelSearch(label, val, Sql.Gte),
  gt: (label, val) => genericSimpleLabelSearch(label, val, Sql.Gt),
  le: (label, val) => genericSimpleLabelSearch(label, val, Sql.Lte),
  lt: (label, val) => genericSimpleLabelSearch(label, val, Sql.Lt)
}

module.exports.extraLabelsWhere = {
  eq: (label, val) => genericExtraLabelSearch(label, val, Sql.Eq),
  neq: (label, val) => genericExtraLabelSearch(label, val, Sql.Ne),
  ge: (label, val) => genericExtraLabelSearch(label, val, Sql.Gte),
  gt: (label, val) => genericExtraLabelSearch(label, val, Sql.Gt),
  le: (label, val) => genericExtraLabelSearch(label, val, Sql.Lte),
  lt: (label, val) => genericExtraLabelSearch(label, val, Sql.Lt)
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
 * @param query {Select}
 * @returns {Select}
 */
module.exports.eq = (token, query) => {
  return genericReq(token, query, 'eq')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.neq = (token, query) => {
  return genericReq(token, query, 'neq')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.gt = (token, query) => {
  return genericReq(token, query, 'gt')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.ge = (token, query) => {
  return genericReq(token, query, 'ge')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.lt = (token, query) => {
  return genericReq(token, query, 'lt')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.le = (token, query) => {
  return genericReq(token, query, 'le')
}
