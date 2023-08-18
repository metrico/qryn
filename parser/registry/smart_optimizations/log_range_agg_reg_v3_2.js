const { getDuration } = require('../common')
const Sql = require('@cloki/clickhouse-sql')
module.exports = {
  /**
   *
   * @param token {Token}
   * @param query {Select}
   * @returns {Select}
   */
  rate: (token, query) => {
    const duration = getDuration(token)
    return genericRate(new Sql.Raw(`toFloat64(countMerge(count)) * 1000 / ${duration}`), token, query)
  },

  /**
   *
   * @param token {Token}
   * @param query {Select}
   * @returns {Select}
   */
  count_over_time: (token, query) => {
    return genericRate(new Sql.Raw('toFloat64(countMerge(count))'), token, query)
  },

  /**
   *
   * @param token {Token}
   * @param query {Select}
   * @returns {Select}
   */
  bytes_rate: (token, query) => {
    const duration = getDuration(token, query)
    return genericRate(new Sql.Raw(`toFloat64(sum(bytes) * 1000 / ${duration})`), token, query)
  },
  /**
   *
   * @param token {Token}
   * @param query {Select}
   * @returns {Select}
   */
  bytes_over_time: (token, query) => {
    return genericRate(new Sql.Raw('toFloat64(sum(bytes))'), token, query)
  }
}

const genericRate = (valueExpr, token, query) => {
  const duration = getDuration(token)
  query.ctx.matrix = true
  query.ctx.duration = duration
  query.limit(undefined, undefined)
  const tsGroupingExpr = new Sql.Raw(`intDiv(timestamp_ns, ${duration}000000) * ${duration}`)
  query.select([tsGroupingExpr, 'timestamp_ns'], [valueExpr, 'value'])
    .groupBy('fingerprint', 'timestamp_ns')
    .orderBy(['fingerprint', 'asc'], ['timestamp_ns', 'asc'])
  const step = query.ctx.step
  if (step <= duration) {
    return query
  }
  const rateC = (new Sql.Select())
    .select(
      'labels',
      [new Sql.Raw(`intDiv(timestamp_ns, ${step}) * ${step}`), 'timestamp_ns'],
      [new Sql.Raw('argMin(rate_b.value, rate_b.timestamp_ns)'), 'value']
    )
    .from('rate_b')
    .groupBy('fingerprint', 'timestamp_ns')
    .orderBy(['fingerprint', 'asc'], ['timestamp_ns', 'asc'])
  return rateC.with(new Sql.With('rate_b', query))
}
