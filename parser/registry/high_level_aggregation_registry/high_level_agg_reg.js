const { applyViaStream, hasStream } = require('../common')
const Sql = require('@cloki/clickhouse-sql')
/**
 *
 * @param token {Token}
 * @returns [string, string[]]
 */
function getByWithout (token) {
  return token.Child('by_without')
    ? [
        token.Child('by_without').value.toString().toLowerCase(),
        token.Child('opt_by_without').Children('label').map(c => c.value)
      ]
    : [undefined, undefined]
}

/**
 *
 * @param expression {string}
 * @param stream {(function(Token, Select): Select)}
 * @returns {(function(Token, Select): Select)}
 */
module.exports.genericRequest = (expression, stream) => {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  return (token, query) => {
    if (hasStream(query)) {
      return stream(token, query)
    }
    const [byWithout, labelList] = getByWithout(token)
    if (!byWithout) {
      return query
    }
    const labelsFilterClause = new Sql.Raw(`arrayFilter(x -> x.1 ${byWithout === 'by' ? 'IN' : 'NOT IN'} ` +
            `(${labelList.map(l => `'${l}'`).join(',')}), labels)`)
    query.ctx.matrix = true
    const aggA = new Sql.With('agg_a', query)
    return (new Sql.Select())
      .with(aggA)
      .select(
        [labelsFilterClause, 'labels'],
        'timestamp_ns',
        [new Sql.Raw(expression), 'value'])
      .from(new Sql.WithReference(aggA))
      .groupBy('labels', 'timestamp_ns')
      .orderBy('labels', 'timestamp_ns')
  }
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.streamSum = (token, query) => {
  return applyViaStream(token, query, (sum, e) => {
    sum = sum || 0
    return sum + e.value
  }, (sum) => sum, false)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.streamMin = (token, query) => {
  return applyViaStream(token, query, (sum, e) => {
    return sum ? Math.min(sum.value, e.value) : { value: e.value }
  }, sum => sum.value)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.streamMax = (token, query) => {
  return applyViaStream(token, query, (sum, e) => {
    return sum ? Math.max(sum.value, e.value) : { value: e.value }
  }, sum => sum.value)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.streamAvg = (token, query) => {
  return applyViaStream(token, query, (sum, e) => {
    return sum ? { value: sum.value + e.value, count: sum.count + 1 } : { value: e.value, count: 1 }
  }, sum => sum.value / sum.count)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.streamStddev = (token, query) => {
  throw new Error('Not implemented')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.streamStdvar = (token, query) => {
  throw new Error('Not implemented')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.streamCount = (token, query) => {
  return applyViaStream(token, query, (sum) => {
    return sum ? sum + 1 : 1
  }, sum => sum)
}
