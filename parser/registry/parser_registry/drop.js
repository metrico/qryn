const { hasExtraLabels, patchCol, addStream } = require('../common')
const Sql = require('@cloki/clickhouse-sql')
/**
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
const viaClickhouseQuery = (token, query) => {
  const labelsToDrop = token.Children('label').map(l => l.value)
  const colsToPatch = ['labels']
  if (hasExtraLabels(query)) {
    colsToPatch.push('extra_labels')
  }
  for (const colName of colsToPatch) {
    patchCol(query, colName, (col) => {
      const colVal = new Sql.Raw('')
      colVal.toString = () =>
        `arrayFilter(x -> x.1 NOT IN (${labelsToDrop.map(Sql.quoteVal).join(',')}), ${col})`
      return [colVal, colName]
    })
  }
  return query
}

/**
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
const viaStream = (token, query) => {
  const labelsToDrop = token.Children('label').map(l => l.value)
  addStream(query, (s) => s.map(e => {
    if (!e.labels) {
      return e
    }
    for (const l of labelsToDrop) {
      delete e.labels[l]
    }
    return e
  }))
  return query
}

module.exports = {
  viaClickhouseQuery,
  viaStream
}
