const { getDuration, hasStream } = require('../common')
const reg = require('./log_range_agg_reg')
const { genericRate } = reg
const Sql = require('clickhouse-sql')

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  rate: (token, query) => {
    if (hasStream(query)) {
      return reg.rateStream(token, query)
    }
    const duration = getDuration(token)
    return genericRate(new Sql.Raw(`toFloat64(count(1)) * 1000 / ${duration}`), token, query)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  count_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.countOverTimeStream(token, query)
    }
    return genericRate(new Sql.Raw('toFloat64(count(1))'), token, query)
  },

  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  bytes_rate: (token, query) => {
    if (hasStream(query)) {
      return reg.bytesRateStream(token, query)
    }
    const duration = getDuration(token, query)
    return genericRate(new Sql.Raw(`toFloat64(sum(length(string))) * 1000 / ${duration}`), token, query)
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  bytes_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.bytesOverTimeStream(token, query)
    }
    return genericRate(new Sql.Raw('toFloat64(sum(length(string)))'), token, query)
  },
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  absent_over_time: (token, query) => {
    if (hasStream(query)) {
      throw new Error('Not implemented')
    }
    query.ctx.matrix = true
    const duration = getDuration(token)
    query.select_list = []
    query.select('labels',
      [new Sql.Raw(`toUInt64(intDiv(timestamp_ms, ${duration}) * ${duration})`), 'timestamp_ms'],
      [new Sql.Raw('toFloat64(0)'), 'value'])
    query.limit(undefined, undefined)
    query.groupBy('labels', 'timestamp_ms')
    query.orderBy(['labels', 'asc'], ['timestamp_ms', 'asc'])
    query.ctx.matrix = true
    const data = new Sql.With('rate_a', query)
    const numbers = new Sql.Raw('')
    numbers.toString = () =>
        `numbers(${Math.floor((query.getParam('to').get() -
          query.getParam('from').get()) / duration)})`
    const selectNumbers = new Sql.Raw('')
    selectNumbers.toString = () => `number * ${duration} + ${query.getParam('from').get()}`
    const gaps = (new Sql.Select())
      .select('a1.labels',
        [selectNumbers, 'timestamp_ms'],
        [new Sql.Raw('toFloat64(1)'), 'value']
      )
      .from(
        [(new Sql.Select()).select('labels').from(new Sql.WithReference(query.withs.str_sel)), 'a1'],
        [numbers, 'a2']
      )
    const res = (new Sql.Select())
      .with(data)
      .select('labels', 'timestamp_ms', [new Sql.Raw('min(value)'), 'value'])
      .from(new Sql.UnionAll(
        (new Sql.Select())
          .from([new Sql.WithReference(data), 'rate_a']),
        gaps
      ))
      .groupBy('labels', 'timestamp_ms')
      .orderBy('labels', 'timestamp_ms')
    return res

    /* {
      ctx: query.ctx,
      with: {
        rate_a: queryData,
        rate_b: queryGaps,
        rate_c: { requests: [{ select: ['*'], from: 'rate_a' }, { select: ['*'], from: 'rate_b' }] }
      },
      select: ['labels', 'timestamp_ms', 'min(value) as value'], // other than the generic
      from: 'rate_c',
      group_by: ['labels', 'timestamp_ms'],
      order_by: {
        name: ['labels', 'timestamp_ms'],
        order: 'asc'
      }
    } */
  }
}
