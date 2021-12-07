const { map, hasExtraLabels, hasStream, addStream } = require('./common')
const Sql = require('clickhouse-sql')
/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports = (token, query) => {
  const label = token.Child('label').value
  if (hasStream(query)) {
    return viaStream(label, query)
  }
  if (label === '_entry') {
    return unwrapLine(query)
  }
  if (hasExtraLabels(query)) {
    return viaQueryWithExtraLabels(label, query)
  }
  return viaQuery(label, query)
}

/**
 *
 * @param query {Select}
 * @returns {Select}
 */
function unwrapLine (query) {
  return query.select([new Sql.Raw('toFloat64OrNull(string)'), 'unwrapped'])
    .where(Sql.Eq(new Sql.Raw('isNotNull(unwrapped)'), 1))
}

/**
 *
 * @param label {string}
 * @param query {Select}
 * @returns {Select}
 */
function viaQuery (label, query) {
  return query.select(
    [new Sql.Raw(`toFloat64OrNull(JSONExtractString(labels,'${label}'))`, 'unwrapped')]
  ).where(
    Sql.Eq(new Sql.Raw(`JSONHas(labels, '${label}')`), 1),
    Sql.Eq(new Sql.Raw('isNotNull(unwrapped)'), 1)
  )
}

/**
 *
 * @param label {string}
 * @param query {Select}
 * @returns {Select}
 */
function viaQueryWithExtraLabels (label, query) {
  return query.select(
    [new Sql.Raw(`toFloat64OrNull(if(arrayExists(x -> x.1 == '${label}', extra_labels), ` +
      `arrayFirst(x -> x.1 == '${label}', extra_labels).2, ` +
      `JSONExtractString(labels,'${label}')))`), 'unwrapped']
  ).where(Sql.Or(
    Sql.Ne(new Sql.Raw(`arrayFirstIndex(x -> x.1 == '${label}', extra_labels)`), 0),
    Sql.Eq(new Sql.Raw(`JSONHas(labels, '${label}')`), 1)
  ), Sql.Eq(new Sql.Raw('isNotNull(unwrapped)'), 1))
}

/**
 *
 * @param label {string}
 * @param query {Select}
 * @returns {Select}
 */
function viaStream (label, query) {
  const isUnwrapString = label === '_entry'
  return addStream(query, (stream) => map(stream, e => {
    if (!e || !e.labels) {
      return { ...e }
    }
    if (!isUnwrapString && !e.labels[label]) {
      return null
    }
    try {
      e.unwrapped = parseFloat(isUnwrapString ? e.string : e.labels[label])
      if (isNaN(e.unwrapped)) {
        return null
      }
      return e
    } catch (e) {
      return null
    }
  }).filter(e => e))
}
