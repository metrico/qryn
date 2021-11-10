const { _and, unquoteToken, querySelectorPostProcess, isEOF } = require('../common')
const { DATABASE_NAME } = require('../../../lib/utils')
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
 * @returns {registry_types.Request}
 */
const streamSelectQuery = () => {
  return {
    select: ['fingerprint'],
    from: `${DATABASE_NAME()}.time_series`,
    where: ['AND']
  }
}

/**
 * @param query {registry_types.Request}
 * @param clauses {string[]}
 * @returns {registry_types.Request}
 */
module.exports.simpleAnd = (query, clauses) => {
  const isStrSel = query.with && query.with.str_sel
  let strSel = isStrSel ? query.with.str_sel : streamSelectQuery()
  strSel = _and(strSel, clauses)
  query = {
    ...query,
    with: {
      ...(query.with || {}),
      str_sel: strSel
    }
  }
  if (isStrSel) {
    return query
  }
  return querySelectorPostProcess(_and(query, ['samples.fingerprint IN str_sel']))
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.neqSimple = (token, query) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(false, false, label, value)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.neqExtraLabels = (token, query) => {
  const [label, value] = labelAndVal(token)
  return [['OR', `arrayExists(x -> x.1 == '${label}' AND x.2 != '${value}', extra_labels) != 0`,
    [
      'AND',
                `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                ...selectorClauses(false, false, label, value)
    ]
  ]]
}

/**
 *
 * @param s {DataStream}
 * @param fn {function(Object): boolean}
 */
function filter (s, fn) { // TODO: Are we using this function?
  return s.filter(e => (e && e.labels && fn(e)) || isEOF(e))
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.neqStream = (token, query) => {
  const [label, value] = labelAndVal(token)
  return (e) => e.labels[label] && e.labels[label] !== value
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.nregSimple = (token, query) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(true, false, label, value)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.nregExtraLabels = (token, query) => {
  const [label, value] = labelAndVal(token)

  return [['OR', `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') == [], extra_labels) != 0`,
    [
      'AND',
                `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                ...selectorClauses(true, true, label, value)
    ]
  ]]
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.nregStream = (token, query) => {
  const [label, value] = labelAndVal(token)
  const re = new RegExp(value)
  return (e) => e.labels[label] && !e.labels[label].match(re)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.regSimple = (token, query) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(true, true, label, value)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.regExtraLabels = (token, query) => {
  const [label, value] = labelAndVal(token)

  return [['OR', `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') != [], extra_labels) != 0`,
    [
      'AND',
                `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                ...selectorClauses(true, true, label, value)
    ]
  ]]
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.regStream = (token, query) => {
  const [label, value] = labelAndVal(token)
  const re = new RegExp(value)
  return (e) => e.EOF || (e && e.labels && e.labels[label] && e.labels[label].match(re))
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.eqSimple = (token, query) => {
  const [label, value] = labelAndVal(token)
  return selectorClauses(false, true, label, value)
}
/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
module.exports.eqExtraLabels = (token, query) => {
  const [label, value] = labelAndVal(token)

  return [['OR', `indexOf(extra_labels, ('${label}', '${value}')) > 0`,
    [
      'AND',
        `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
        ...selectorClauses(false, true, label, value)
    ]
  ]]
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.eqStream = (token, query) => {
  const [label, value] = labelAndVal(token)
  return (e) => e.EOF || (e && e.labels && e.labels[label] && e.labels[label] === value)
}
