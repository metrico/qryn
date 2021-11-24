const { getDuration } = require('../common')
const reg = require('./log_range_agg_reg')
const { genericRate } = reg

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  rate: (token, query) => {
    if (query.stream && query.stream.length) {
      return reg.rateStream(token, query)
    }
    const duration = getDuration(token, query)
    return genericRate(`toFloat64(count(1)) * 1000 / ${duration}`, token, query)
  },

  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  count_over_time: (token, query) => {
    if (query.stream && query.stream.length) {
      return reg.countOverTimeStream(token, query)
    }
    return genericRate('toFloat64(count(1))', token, query)
  },

  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  bytes_rate: (token, query) => {
    if (query.stream && query.stream.length) {
      return reg.bytesRateStream(token, query)
    }
    const duration = getDuration(token, query)
    return genericRate(`toFloat64(sum(length(string))) * 1000 / ${duration}`, token, query)
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  bytes_over_time: (token, query) => {
    if (query.stream && query.stream.length) {
      return reg.bytesOverTimeStream(token, query)
    }
    return genericRate('toFloat64(sum(length(string)))', token, query)
  },
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  absent_over_time: (token, query) => {
    if (query.stream && query.stream.length) {
      return reg.bytesOverTimeStream(token, query)
    }
    const duration = getDuration(token, query)
    const queryData = { ...query }
    queryData.with = undefined
    queryData.select = ['labels', `floor(timestamp_ms / ${duration}) * ${duration} as timestamp_ms`,
      'toFloat64(0) as value']
    queryData.limit = undefined
    queryData.group_by = ['labels', 'timestamp_ms']
    queryData.order_by = {
      name: ['labels', 'timestamp_ms'],
      order: 'asc'
    }
    queryData.matrix = true
    /**
         *
         * @type {registry_types.Request}
         */
    const queryGaps = {
      select: [
        'a1.labels',
                `toFloat64(${Math.floor(query.ctx.start / duration) * duration} + number * ${duration}) as timestamp_ms`,
                'toFloat64(1) as value' // other than the generic
      ],
      from: `(SELECT DISTINCT labels FROM rate_a) as a1, numbers(${Math.floor((query.ctx.end - query.ctx.start) / duration)}) as a2`
    }
    return {
      ctx: query.ctx,
      with: {
        ...query.with,
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
      },
      matrix: true
    }
  }
}
