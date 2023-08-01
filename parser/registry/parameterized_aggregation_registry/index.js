const { hasStream } = require('../common')
const { QrynBadRequest } = require('../../../lib/handlers/errors')
const Sql = require('@cloki/clickhouse-sql')

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @param isTop {boolean}
 * @returns {Select}
 */
const topBottom = (token, query, isTop) => {
  if (hasStream(query)) {
    throw new QrynBadRequest('Not supported')
  }

  const parA = new Sql.With('par_a', query)
  const len = parseInt(token.Child('parameter_value').value)
  const lambda = isTop ? 'x -> (-x.1, x.2), ' : ''
  const q1 = (new Sql.Select())
    .with(parA)
    .select(
      ['par_a.timestamp_ns', 'timestamp_ns'],
      [new Sql.Raw(
        `arraySlice(arraySort(${lambda}groupArray((par_a.value, par_a.labels))), 1, ${len})`), 'slice']
    ).from(new Sql.WithReference(parA))
    .groupBy('timestamp_ns')

  const parB = new Sql.With('par_b', q1)
  return (new Sql.Select())
    .with(parB)
    .select(
      [new Sql.Raw('arr_b.1'), 'value'],
      [new Sql.Raw('arr_b.2'), 'labels'],
      ['par_b.timestamp_ns', 'timestamp_ns']
    )
    .from(new Sql.WithReference(parB))
    .join(['par_b.slice', 'arr_b'], 'array')
    .orderBy('labels', 'timestamp_ns')
}

module.exports = {
  /**
   *
   * @param token {Token}
   * @param query {Select}
   * @returns {Select}
   */
  topk: (token, query) => {
    return topBottom(token, query, true)
  },

  /**
   *
   * @param token {Token}
   * @param query {Select}
   * @returns {Select}
   */
  bottomk: (token, query) => {
    return topBottom(token, query, false)
  }
}
