const { unquoteToken, isEOF } = require('../common')
const Sql = require('@cloki/clickhouse-sql')
/**
 * @param regex {boolean}
 * @param eq {boolean}
 * @param label {string}
 * @param value {string}
 * @returns {Conditions}
 */
function selectorClauses (regex, eq, label, value) {
  const call = regex
    ? [new Sql.Raw(`extractAllGroups(JSONExtractString(labels, '${label}'), '(${value})')`),
        '[]', eq ? Sql.Ne : Sql.Eq]
    : [new Sql.Raw(`JSONExtractString(labels, '${label}')`), value, eq ? Sql.Eq : Sql.Ne]
  return Sql.And(
    Sql.Eq(new Sql.Raw(`JSONHas(labels, '${label}')`), 1),
    call[2](call[0], call[1])
  ) /* [
        `JSONHas(labels, '${label}')`,
        regex
          ? `extractAllGroups(JSONExtractString(labels, '${label}'), '(${value})') ${eq ? '!=' : '=='} []`
          : `JSONExtractString(labels, '${label}') ${eq ? '=' : '!='} '${value}'`
  ] */
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
  const param = query.getParam('timeSeriesTable') || new Sql.Parameter('timeSeriesTable')
  query.addParam(param)
  return new Sql.With(
    'str_sel',
    (new Sql.Select())
      .select('fingerprint')
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
  /* query.joins = query.joins.filter(j => j.table[1] !== 'time_series')
  query.join([new Sql.WithReference(strSel), 'time_series'], 'left',
    Sql.Eq('samples.fingerprint', Sql.quoteTerm('time_series.fingerprint'))) */
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
 * @returns {Conditions}
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
  return Sql.Or(
    new Sql.Ne(new Sql.Raw(`arrayExists(x -> x.1 == '${label}' AND x.2 != '${value}', extra_labels)`), 0),
    Sql.And(
      Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == '${label}', extra_labels)`), 0),
      selectorClauses(false, false, label, value)
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
 * @returns {Conditions}
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
  return Sql.Or(
    Sql.Eq(
      new Sql.Raw(
        `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') == [], extra_labels)`), 0
    ),
    Sql.And(
      Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == '${label}', extra_labels)`), 0),
      selectorClauses(true, true, label, value)))
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
 * @returns {Conditions}
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

  return Sql.Or(
    Sql.Eq(
      new Sql.Raw(
        `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') != [], extra_labels)`), 0
    ),
    Sql.And(`arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
      selectorClauses(true, true, label, value)))
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
 * @returns {Conditions}
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

  return Sql.Or(
    Sql.Gt(new Sql.Raw(`indexOf(extra_labels, ('${label}', '${value}'))`), 0),
    Sql.And(
      Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == '${label}', extra_labels)`), 0),
      selectorClauses(false, true, label, value)))
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
