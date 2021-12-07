const { getDuration, concatLabels, applyViaStream } = require('../common')
const Sql = require('clickhouse-sql')

/**
 *
 * @param valueExpr {SQLObject}
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
const genericRate = (valueExpr, token, query) => {
  const duration = getDuration(token)
  query.ctx.matrix = true
  query.limit(undefined, undefined)
  const step = query.ctx.step
  const rateA = new Sql.With('rate_a', query)
  const rateB = (new Sql.Select())
    .select(
      [concatLabels(query), 'labels'],
      [new Sql.Raw(`floor(timestamp_ms / ${duration}) * ${duration}`), 'timestamp_ms'],
      [valueExpr, 'value']
    )
    .from(new Sql.WithReference(rateA))
    .groupBy('labels', 'timestamp_ms')
    .orderBy(['labels', 'asc'], ['timestamp_ms', 'asc'])
  if (step <= duration) {
    return rateB.with(rateA)
  }
  const rateC = (new Sql.Select())
    .select(
      'labels',
      [new Sql.Raw(`floor(timestamp_ms / ${step}) * ${step}`), 'timestamp_ms'],
      [new Sql.Raw('argMin(rate_b.value, rate_b.timestamp_ms)'), 'value']
    )
    .from('rate_b')
    .groupBy('labels', 'timestamp_ms')
    .orderBy(['labels', 'asc'], ['timestamp_ms', 'asc'])
  return rateC.with(rateA, new Sql.With('rate_b', rateB))
}

module.exports.genericRate = genericRate

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.rateStream = (token, query) => {
  const duration = getDuration(token, query)
  return applyViaStream(token, query, (sum) => {
    sum = sum || 0
    ++sum
    return sum
  }, (sum) => sum * 1000 / duration, false)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.countOverTimeStream = (token, query) => {
  return applyViaStream(token, query, (sum) => {
    sum = sum || 0
    ++sum
    return sum
  }, (sum) => sum, false)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.bytesRateStream = (token, query) => {
  const duration = getDuration(token)
  return applyViaStream(token, query, (sum, entry) => {
    sum = sum || 0
    sum += entry.string.length
    return sum
  }, (sum) => sum * 1000 / duration, false)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.bytesOverTimeStream = (token, query) => {
  return applyViaStream(token, query, (sum, entry) => {
    sum = sum || 0
    sum += entry.string.length
    return sum
  }, (sum) => sum, false)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.bytesOverTimeStream = (token, query) => {
  throw new Error('Not Implemented')
}
