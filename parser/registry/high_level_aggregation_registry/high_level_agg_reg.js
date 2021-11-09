const { applyViaStream } = require('../common')

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
 * @param stream {(function(Token, registry_types.Request): registry_types.Request)}
 * @returns {(function(Token, registry_types.Request): registry_types.Request)}
 */
module.exports.genericRequest = (expression, stream) => {
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  return (token, query) => {
    if (query.stream && query.stream.length) {
      return stream(token, query)
    }
    const [byWithout, labelList] = getByWithout(token)
    if (!byWithout) {
      return query
    }
    const labelsFilterClause = `arrayFilter(x -> x.1 ${byWithout === 'by' ? 'IN' : 'NOT IN'} ` +
            `(${labelList.map(l => `'${l}'`).join(',')}), labels)`
    return {
      ctx: query.ctx,
      with: {
        ...(query.with ? query.with : {}),
        agg_a: {
          ...query,
          ctx: undefined,
          with: undefined,
          stream: undefined
        }
      },
      select: [
                `${labelsFilterClause} as labels`,
                'timestamp_ms',
                `${expression} as value` // 'sum(value) as value'
      ],
      from: 'agg_a',
      group_by: ['labels', 'timestamp_ms'],
      order_by: {
        name: ['labels', 'timestamp_ms'],
        order: 'asc'
      },
      matrix: true,
      stream: query.stream
    }
  }
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
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
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.streamMin = (token, query) => {
  return applyViaStream(token, query, (sum, e) => {
    return sum ? Math.min(sum.value, e.value) : { value: e.value }
  }, sum => sum.value)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.streamMax = (token, query) => {
  return applyViaStream(token, query, (sum, e) => {
    return sum ? Math.max(sum.value, e.value) : { value: e.value }
  }, sum => sum.value)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.streamAvg = (token, query) => {
  return applyViaStream(token, query, (sum, e) => {
    return sum ? { value: sum.value + e.value, count: sum.count + 1 } : { value: e.value, count: 1 }
  }, sum => sum.value / sum.count)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.streamStddev = (token, query) => {
  throw new Error('Not implemented')
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.streamStdvar = (token, query) => {
  throw new Error('Not implemented')
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.streamCount = (token, query) => {
  return applyViaStream(token, query, (sum) => {
    return sum ? sum + 1 : 1
  }, sum => sum)
}
