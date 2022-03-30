const { getDuration, hasStream } = require('../common')
const reg = require('./log_range_agg_reg')
const { genericRate } = reg
const Sql = require('@cloki/clickhouse-sql')
const { addStream } = require('../common')
const JSONstringify = require('json-stable-stringify')

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
      [new Sql.Raw(`toUInt64(intDiv(timestamp_ns, ${duration}000000) * ${duration})`), 'timestamp_ns'],
      [new Sql.Raw('toFloat64(0)'), 'value'])
    query.limit(undefined, undefined)
    query.groupBy('labels', 'timestamp_ns')
    query.orderBy(['labels', 'asc'], ['timestamp_ns', 'asc'])
    query.ctx.matrix = true
    let nextTS = query.ctx.start
    let lastLabels = null
    return addStream(query, (s) => s.remap((emit, val) => {
      if (val.EOF && lastLabels) {
        const lbls = JSON.parse(lastLabels)
        for (let i = parseInt(nextTS); i < parseInt(query.ctx.end); i += duration) {
          emit({ labels: lbls, value: 1, timestamp_ns: i })
        }
        emit(val)
        return
      }
      if (!val.labels) {
        emit(val)
        return
      }
      if (JSONstringify(val.labels) !== lastLabels) {
        if (lastLabels) {
          const lbls = JSON.parse(lastLabels)
          for (let i = parseInt(nextTS); i < parseInt(query.ctx.end); i += duration) {
            emit({ labels: lbls, value: 1, timestamp_ns: i })
          }
        }
        nextTS = query.ctx.start
        lastLabels = JSONstringify(val.labels)
      }
      for (let i = parseInt(nextTS); i < val.timestamp_ns; i += duration) {
        emit({ ...val, value: 1, timestamp_ns: i })
      }
      emit(val)
      nextTS = parseInt(val.timestamp_ns) + duration
    }))

    /* {
      ctx: query.ctx,
      with: {
        rate_a: queryData,
        rate_b: queryGaps,
        rate_c: { requests: [{ select: ['*'], from: 'rate_a' }, { select: ['*'], from: 'rate_b' }] }
      },
      select: ['labels', 'timestamp_ns', 'min(value) as value'], // other than the generic
      from: 'rate_c',
      group_by: ['labels', 'timestamp_ns'],
      order_by: {
        name: ['labels', 'timestamp_ns'],
        order: 'asc'
      }
    } */
  }
}
