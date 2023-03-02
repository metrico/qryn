const { getDuration, concatLabels, timeShiftViaStream } = require('../common')
const _applyViaStream = require('../common').applyViaStream
const Sql = require('@cloki/clickhouse-sql')

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @param counterFn {function(any, any, number): any}
 * @param summarizeFn {function(any): number}
 * @param lastValue {boolean} if the applier should take the latest value in step (if step > duration)
 * @param byWithoutName {string} name of the by_without token
 */
const applyViaStream = (token, query, counterFn, summarizeFn, lastValue, byWithoutName) => {
  query.limit(undefined, undefined)
  query.ctx.matrix = true
  return _applyViaStream(token, timeShiftViaStream(token, query), counterFn, summarizeFn, lastValue, byWithoutName)
}

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
  query.ctx.duration = duration
  query.limit(undefined, undefined)
  const step = query.ctx.step
  const rateA = new Sql.With('rate_a', query)
  const tsMoveParam = new Sql.Parameter('timestamp_shift')
  query.addParam(tsMoveParam)
  const tsGroupingExpr = new Sql.Raw('')
  tsGroupingExpr.toString = () => {
    if (!tsMoveParam.get()) {
      return `intDiv(timestamp_ns, ${duration}) * ${duration}`
    }
    return `intDiv(timestamp_ns - ${tsMoveParam.toString()}, ${duration}) * ${duration} + ${tsMoveParam.toString()}`
  }
  const rateB = (new Sql.Select())
    .select(
      [concatLabels(query), 'labels'],
      [tsGroupingExpr, 'timestamp_ns'],
      [valueExpr, 'value']
    )
    .from(new Sql.WithReference(rateA))
    .groupBy('labels', 'timestamp_ns')
    .orderBy(['labels', 'asc'], ['timestamp_ns', 'asc'])
  if (step <= duration) {
    return rateB.with(rateA)
  }
  const rateC = (new Sql.Select())
    .select(
      'labels',
      [new Sql.Raw(`intDiv(timestamp_ns, ${step}) * ${step}`), 'timestamp_ns'],
      [new Sql.Raw('argMin(rate_b.value, rate_b.timestamp_ns)'), 'value']
    )
    .from('rate_b')
    .groupBy('labels', 'timestamp_ns')
    .orderBy(['labels', 'asc'], ['timestamp_ns', 'asc'])
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
