const { isEOF, sharedParamNames } = require('../common')
const { labelAndVal } = require('./common')
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
    ? [new Sql.Raw(`match(arrayFirst(x -> x.1 == ${Sql.quoteVal(label)}, labels).2, ${Sql.quoteVal(value)})`),
        0, eq ? Sql.Ne : Sql.Eq]
    : [new Sql.Raw(`arrayFirst(x -> x.1 == ${Sql.quoteVal(label)}, labels).2`), value, eq ? Sql.Eq : Sql.Ne]
  return Sql.And(
    Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == ${Sql.quoteVal(label)}, labels)`), 1),
    call[2](call[0], call[1])
  )
}

function simpleSelectorClauses (regex, eq, label, value) {
  const call = regex
    ? [new Sql.Raw(`extractAllGroups(JSONExtractString(labels, ${Sql.quoteVal(label)}), ${Sql.quoteVal('(' + value + ')')})`),
        '[]', eq ? Sql.Ne : Sql.Eq]
    : [new Sql.Raw(`JSONExtractString(labels, ${Sql.quoteVal(label)})`), value, eq ? Sql.Eq : Sql.Ne]
  return Sql.And(
    Sql.Eq(new Sql.Raw(`JSONHas(labels, ${Sql.quoteVal(label)})`), 1),
    call[2](call[0], call[1])
  ) /* [
        `JSONHas(labels, '${label}')`,
        regex
          ? `extractAllGroups(JSONExtractString(labels, '${label}'), '(${value})') ${eq ? '!=' : '=='} []`
          : `JSONExtractString(labels, '${label}') ${eq ? '=' : '!='} '${value}'`
  ] */
}

/**
 * @param query {Select}
 * @returns {With}
 */
const streamSelectQuery = (query) => {
  const param = query.getParam(sharedParamNames.timeSeriesTable) ||
      new Sql.Parameter(sharedParamNames.timeSeriesTable)
  query.addParam(param)
  const res = new Sql.With(
    'str_sel',
    (new Sql.Select())
      .select('fingerprint')
      .distinct(true)
      .from(param), query.ctx.inline)
  if (query.with() && query.with().idx_sel) {
    res.query = res.query.where(new Sql.In('fingerprint', 'in', new Sql.WithReference(query.with().idx_sel)))
  }
  return res
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
  return simpleSelectorClauses(false, false, label, value)
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
    new Sql.Ne(new Sql.Raw(`arrayExists(x -> x.1 == ${Sql.quoteVal(label)} AND x.2 != ${Sql.quoteVal(value)}, extra_labels)`), 0),
    Sql.And(
      Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == ${Sql.quoteVal(label)}, extra_labels)`), 0),
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
  return (e) => isEOF(e) || (e && e.labels && e.labels[label] && e.labels[label] !== value)
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {Conditions}
 */
module.exports.nregSimple = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return simpleSelectorClauses(true, false, label, value)
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
    Sql.Ne(
      new Sql.Raw(
        `arrayExists(x -> x.1 == '${label}' AND match(x.2, '${value}') == 0, extra_labels)`), 0
    ),
    Sql.And(
      Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == ${Sql.quoteVal(label)}, extra_labels)`), 0),
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
  return (e) => isEOF(e) || (e && e.labels && e.labels[label] && !e.labels[label].match(re))
}

/**
 *
 * @param token {Token}
 * //@param query {registry_types.Request}
 * @returns {Conditions}
 */
module.exports.regSimple = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return simpleSelectorClauses(true, true, label, value)
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
    Sql.Ne(
      new Sql.Raw(
        `arrayExists(x -> x.1 == '${label}' AND match(x.2, '${value}') != 0, extra_labels)`), 0
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
  return simpleSelectorClauses(false, true, label, value)
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
    Sql.Gt(new Sql.Raw(`indexOf(extra_labels, (${Sql.quoteVal(label)}, ${Sql.quoteVal(value)}))`), 0),
    Sql.And(
      Sql.Eq(new Sql.Raw(`arrayExists(x -> x.1 == ${Sql.quoteVal(label)}, extra_labels)`), 0),
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
