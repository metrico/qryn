const { _and, map, hasExtraLabels } = require('./common')
/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports = (token, query) => {
  const label = token.Child('label').value
  if (query.stream) {
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
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
function unwrapLine (query) {
  query = {
    ...query,
    select: [...query.select, 'toFloat64OrNull(string) as unwrapped']
  }
  return _and(query, [
    'isNotNull(unwrapped)'
  ])
}

/**
 *
 * @param label {string}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
function viaQuery (label, query) {
  query = {
    ...query,
    select: [...query.select, `toFloat64OrNull(JSONExtractString(labels,'${label}')) as unwrapped`]
  }
  return _and(query, [
        `JSONHas(labels, '${label}')`,
        'isNotNull(unwrapped)'
  ])
}

/**
 *
 * @param label {string}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
function viaQueryWithExtraLabels (label, query) {
  query = {
    ...query,
    select: [...query.select, `toFloat64OrNull(if(arrayExists(x -> x.1 == '${label}', extra_labels), ` +
                `arrayFirst(x -> x.1 == '${label}', extra_labels).2, ` +
            `JSONExtractString(labels,'${label}'))) as unwrapped`]
  }
  return _and(query, [[
    'OR',
        `arrayFirstIndex(x -> x.1 == '${label}', extra_labels) != 0`,
        `JSONHas(labels, '${label}')`
  ], 'isNotNull(unwrapped)'])
}

/**
 *
 * @param label {string}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
function viaStream (label, query) {
  const isUnwrapString = label === '_entry'
  return {
    ...query,
    stream: [
      ...(query.stream ? query.stream : []),
      /**
             *
             * @param stream {DataStream}
             */
      (stream) => map(stream, e => {
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
      }).filter(e => e)
    ]
  }
}
