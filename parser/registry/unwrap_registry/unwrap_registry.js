const { getDuration, concatLabels, applyViaStream } = require('../common')

/**
 *
 * @param viaRequest {function(Token, registry_types.Request): registry_types.Request}
 * @param viaStream {function(Token, registry_types.Request): registry_types.Request}
 * @returns {{
 *  viaRequest: (function(Token, registry_types.Request): registry_types.Request),
 *  viaStream: (function(Token, registry_types.Request): registry_types.Request)
 *  }}
 */
function builder (viaRequest, viaStream) {
  return {
    viaRequest: viaRequest,
    viaStream: viaStream
  }
}

/**
 * sum_over_time(unwrapped-range): the sum of all values in the specified interval.
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string}
 */
function applyByWithoutLabels (token, query) {
  let labels = concatLabels(query)
  const filterLabels = token.Children('label').map(l => l.value).map(l => `'${l}'`)
  if (token.Child('by_without_unwrap').value === 'by') {
    labels = `arraySort(arrayFilter(x -> arrayExists(y -> x.1 == y, [${filterLabels.join(',')}]) != 0, ` +
            `${labels}))`
  }
  if (token.Child('by_without_unwrap').value === 'without') {
    labels = `arraySort(arrayFilter(x -> arrayExists(y -> x.1 == y, [${filterLabels.join(',')}]) == 0, ` +
            `${labels}))`
  }
  return labels
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param valueExpr {string}
 * @param lastValue {boolean} if the applier should take the latest value in step (if step > duration)
 * @returns {registry_types.Request}
 */
function applyViaRequest (token, query, valueExpr, lastValue) {
  const labels = token.Child('by_without_unwrap')
    ? applyByWithoutLabels(token.Child('opt_by_without_unwrap'), query)
    : concatLabels(query)
  const duration = getDuration(token, query)
  const step = query.ctx.step
  /**
     *
     * @type {registry_types.Request}
     */
  const groupingQuery = {
    select: [
            `${labels} as labels`,
            `floor(timestamp_ms / ${duration}) * ${duration} as timestamp_ms`,
            `${valueExpr} as value`
    ],
    from: 'uw_rate_a',
    group_by: ['labels', 'timestamp_ms'],
    order_by: {
      name: ['labels', 'timestamp_ms'],
      order: 'asc'
    }
  }
  const argMin = lastValue ? 'argMin' : 'argMax'
  /**
     *
     * @type {registry_types.Request}
     */
  return {
    stream: query.stream,
    ctx: { ...query.ctx, duration: duration },
    matrix: true,
    with: {
      ...query.with,
      uw_rate_a: {
        ...query,
        stream: undefined,
        with: undefined,
        ctx: undefined,
        matrix: undefined,
        limit: undefined
      },
      uw_rate_b: step > duration ? groupingQuery : undefined
    },
    ...(step > duration
      ? {
          select: [
            'labels', `floor(uw_rate_b.timestamp_ms / ${step}) * ${step} as timestamp_ms`,
                `${argMin}(value,timestamp_ms) as value`
          ],
          from: 'uw_rate_b',
          group_by: ['labels', 'timestamp_ms'],
          order_by: {
            name: ['labels', 'timestamp_ms'],
            order: 'asc'
          }
        }
      : groupingQuery)
  }
}

module.exports = {
  applyViaStream: applyViaStream,
  rate: builder((token, query) => {
    const duration = getDuration(token, query)
    return applyViaRequest(token, query, `SUM(unwrapped) / ${duration / 1000}`)
  }, (token, query) => {
    const duration = getDuration(token, query)
    return applyViaStream(token, query,
      (sum, val) => sum + val.unwrapped,
      (sum) => sum / duration * 1000, false, 'by_without_unwrap')
  }),

  /**
     * sum_over_time(unwrapped-range): the sum of all values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  sumOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'sum(unwrapped)')
  }, (token, query) => {
    return applyViaStream(token, query,
      (sum, val) => sum + val.unwrapped,
      (sum) => sum, false, 'by_without_unwrap')
  }),

  /**
     * avg_over_time(unwrapped-range): the average value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  avgOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'avg(unwrapped)')
  }, (token, query) => {
    return applyViaStream(token, query, (sum, val) => {
      return sum ? { count: sum.count + 1, val: sum.val + val.unwrapped } : { count: 1, val: val.unwrapped }
    }, (sum) => sum.val / sum.count, false, 'by_without_unwrap')
  }),
  /**
     * max_over_time(unwrapped-range): the maximum value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  maxOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'max(unwrapped)')
  }, (token, query) => {
    return applyViaStream(token, query, (sum, val) => {
      return Math.max(sum, val.unwrapped)
    }, (sum) => sum, false, 'by_without_unwrap')
  }),
  /**
     * min_over_time(unwrapped-range): the minimum value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  minOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'min(unwrapped)')
  }, (token, query) => {
    return applyViaStream(token, query, (sum, val) => {
      return Math.min(sum, val.unwrapped)
    }, (sum) => sum, false, 'by_without_unwrap')
  }),
  /**
     * firstOverTime(unwrapped-range): the first value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  firstOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'argMin(unwrapped, uw_rate_a.timestamp_ms)')
  }, (token, query) => {
    return applyViaStream(token, query, (sum, val, time) => {
      return sum && sum.time < time ? sum : { time: time, first: val.unwrapped }
    }, (sum) => sum.first, false, 'by_without_unwrap')
  }),
  /**
     * lastOverTime(unwrapped-range): the last value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  lastOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'argMax(unwrapped, uw_rate_a.timestamp_ms)', true)
  }, (token, query) => {
    return applyViaStream(token, query, (sum, val, time) => {
      return sum && sum.time > time ? sum : { time: time, first: val.unwrapped }
    }, (sum) => sum.first, false, 'by_without_unwrap')
  }),
  /**
     * stdvarOverTime(unwrapped-range): the population standard variance of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  stdvarOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'varPop(unwrapped)')
  }, (token, query) => {
    return applyViaStream(token, query, (/* sum, val */) => {
      throw new Error('not implemented')
    }, (sum) => sum, false, 'by_without_unwrap')
  }),
  /**
     * stddevOverTime(unwrapped-range): the population standard deviation of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  stddevOverTime: builder((token, query) => {
    return applyViaRequest(token, query, 'stddevPop(unwrapped)')
  }, (token, query) => {
    return applyViaStream(token, query, (/* sum, val */) => {
      throw new Error('not implemented')
    }, (sum) => sum, false, 'by_without_unwrap')
  }),
  /**
     * quantileOverTime(scalar,unwrapped-range): the φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.
     * //@param token {Token}
     * //@param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  quantileOverTime: (/* token, query */) => {
    throw new Error('Not implemented')
  },
  /**
     * absentOverTime(unwrapped-range): returns an empty vector if the range vector passed to it has any elements and a 1-element vector with the value 1 if the range vector passed to it has no elements. (absentOverTime is useful for alerting on when no time series and logs stream exist for label combination for a certain amount of time.)
     * //@param token {Token}
     * //@param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  absentOverTime: (/* token, query */) => {
    throw new Error('Not implemented')
  }
}
