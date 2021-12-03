const { unquoteToken, isEOF } = require('../common')
const Sql = require('clickhouse-sql')
/**
 * @param regex {boolean}
 * @param eq {boolean}
 * @param label {string}
 * @param value {string}
 * @returns {string[]}
 */
function selectorClauses (regex, eq, label, value) {
  return [
        `JSONHas(labels, '${label}')`,
        regex
          ? `extractAllGroups(JSONExtractString(labels, '${label}'), '(${value})') ${eq ? '!=' : '=='} []`
          : `JSONExtractString(labels, '${label}') ${eq ? '=' : '!='} '${value}'`
  ]
}

/**
 *
 * @param token {Token}
 * @returns {string[]}
 */
const labelAndVal = (token) => {
  const label = token.Child('label').value
  return [label, unquoteToken(token)]
}

/**
 * @param query {Select}
 * @returns {With}
 */
const streamSelectQuery = (query) => {
  const param = query.getParam('timeSeriesTable') || Sql.Parameter('timeSeriesTable')
  query.addParam(param)
  return new Sql.With(
    'str_sel',
    (new Sql.Select())
      .select('fingerprint', 'labels')
      .distinct(true)
      .from(param)
  )
}

/**
 * @param query {Select}
 * @param clauses {Conditions | string[]}
 * @returns {Select}
 */
module.exports.simpleAnd = (query, clauses) => {
  const isStrSel = query.with() && query.with().str_sel
  /**
   * @type {With}
   */
  const strSel = isStrSel ? query.with().str_sel : streamSelectQuery(query)
  if (Array.isArray(clauses)) {
    strSel.query.where(...clauses)
  } else {
    strSel.query.where(clauses)
  }
  query.with(strSel)
  if (!isStrSel) {
    query.where(new Sql.In('samples.fingerprint', 'in',
      (new Sql.Select()).select('fingerprint').from(new Sql.WithReference(strSel))
    ))
  }
  return query
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.neqSimple = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(false, false, label, value)
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {Conditions}
 */
module.exports.neqExtraLabels = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return new Sql.Or(
    `arrayExists(x -> x.1 == '${label}' AND x.2 != '${value}', extra_labels) != 0`,
    new Sql.And(
      `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
      ...selectorClauses(false, false, label, value)
    ))
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.neqStream = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return (e) => e.labels[label] && e.labels[label] !== value
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.nregSimple = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(true, false, label, value)
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {Conditions}
 */
module.exports.nregExtraLabels = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return new Sql.Or(
    `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') == [], extra_labels) != 0`,
    new Sql.And(
      `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
      ...selectorClauses(true, true, label, value)))
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.nregStream = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  const re = new RegExp(value)
  return (e) => e.labels[label] && !e.labels[label].match(re)
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.regSimple = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(true, true, label, value)
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {Conditions}
 */
module.exports.regExtraLabels = (token/*, query */) => {
  const [label, value] = labelAndVal(token)

  return new Sql.Or(
    `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') != [], extra_labels) != 0`,
    new Sql.And(`arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
      ...selectorClauses(true, true, label, value)))
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.regStream = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  const re = new RegExp(value)
  return (e) => isEOF(e) || (e && e.labels && e.labels[label] && e.labels[label].match(re))
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.eqSimple = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(false, true, label, value)
}
/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {Conditions}
 */
module.exports.eqExtraLabels = (token/*, query */) => {
  const [label, value] = labelAndVal(token)

  return new Sql.Or(
    `indexOf(extra_labels, ('${label}', '${value}')) > 0`,
    new Sql.And(
      `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
      ...selectorClauses(false, true, label, value)))
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.eqStream = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return (e) => isEOF(e) || (e && e.labels && e.labels[label] && e.labels[label] === value)
}
