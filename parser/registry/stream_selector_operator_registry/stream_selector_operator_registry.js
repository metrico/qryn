const { _and, unquoteToken, querySelectorPostProcess, isEOF } = require('../common')
const { DATABASE_NAME, isClustered } = require('../../../lib/utils')

/**
 *
 * @param query {registry_types.Request}
 */
function getIsClustered (query) {
  const qClustered = query.ctx ? query.ctx.clustered : undefined
  return typeof qClustered !== 'undefined' ? qClustered : isClustered
}

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
const streamSelectQuery = (isClustered) => {
  return {
    inline: isClustered,
    select: ['fingerprint', 'labels'],
    distinct: 1,
    from: `${DATABASE_NAME()}.time_series${isClustered ? '_standalone' : ''}`,
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
  const _isClustered = getIsClustered(query)
  let strSel = isStrSel ? query.with.str_sel : streamSelectQuery(_isClustered)
  strSel = _and(strSel, clauses)
  /**
   *
   * @type {registry_types.Request}
   */
  query = {
    ...query,
    with: {
      ...(query.with || {}),
      str_sel: strSel
    },
    depends: {
      str_sel: {
        /**
         *
         * @param query {registry_types.Request}
         * @param str {string}
         * @returns {registry_types.Request}
         */
        on_stringify: (query, str) => {
          if (!_isClustered) { return query }
          query.left_join.find(j => j.name.endsWith('as str_sel')).name = `(${str}) as str_sel`
          query.where = query.where.map(w => w === 'samples.fingerprint IN (SELECT fingerprint FROM str_sel)'
            ? `samples.fingerprint IN (SELECT fingerprint FROM (${str}))`
            : w)
          return query
        }
      }
    },
    select: query.select.map(f => f.replace('time_series', 'str_sel')),
    left_join: !isStrSel
      ? [
          ...(query.left_join || []).filter(j => j.name.indexOf('time_series') === -1),
          {
            name: '(SELECT * FROM str_sel) as str_sel',
            on: ['AND', 'samples.fingerprint = str_sel.fingerprint']
          }
        ]
      : query.left_join
  }
  if (isStrSel) {
    return query
  }
  return querySelectorPostProcess(_and(query,
    ['samples.fingerprint IN (SELECT fingerprint FROM str_sel)']))
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
 * @returns {string[]}
 */
module.exports.neqExtraLabels = (token/*, query */) => {
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
 * @returns {string[]}
 */
module.exports.nregExtraLabels = (token/*, query */) => {
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
 * @returns {string[]}
 */
module.exports.regExtraLabels = (token/*, query */) => {
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
 * @returns {string[]}
 */
module.exports.eqExtraLabels = (token/*, query */) => {
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
 * //@param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
module.exports.eqStream = (token/*, query */) => {
  const [label, value] = labelAndVal(token)
  return (e) => isEOF(e) || (e && e.labels && e.labels[label] && e.labels[label] === value)
}
