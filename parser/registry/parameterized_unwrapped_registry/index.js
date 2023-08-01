const { QrynBadRequest } = require('../../../lib/handlers/errors')
const { hasStream, getDuration } = require('../common')
const Sql = require('@cloki/clickhouse-sql')
const { applyByWithoutLabels } = require('../unwrap_registry/unwrap_registry')

module.exports = {
  /**
   * quantileOverTime(scalar,unwrapped-range): the φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.
   * @param token {Token}
   * @param query {Select}
   * @returns {Select}
   */
  quantile_over_time: (token, query) => {
    if (hasStream(query)) {
      throw new QrynBadRequest('Not supported')
    }
    query.ctx.matrix = true
    const durationMS = getDuration(token)
    query.ctx.duration = durationMS
    const stepMS = query.ctx.step
    const quantVal = parseFloat(token.Child('parameter_value').value)
    const quantA = new Sql.With('quant_a', query)
    const labels = applyByWithoutLabels(token.Child('req_by_without_unwrap'), query)
    const quantB = (new Sql.Select())
      .with(quantA)
      .select(
        [labels, 'labels'],
        [new Sql.Raw(`intDiv(quant_a.timestamp_ns, ${durationMS}) * ${durationMS}`), 'timestamp_ns'],
        [new Sql.Raw(`quantile(${quantVal})(unwrapped)`), 'value']
      ).from(new Sql.WithReference(quantA))
      .groupBy('timestamp_ns', 'labels')
      .orderBy('labels', 'timestamp_ns')
    if (stepMS <= durationMS) {
      return quantB
    }
    const withQuantB = new Sql.With('quant_b', quantB)
    return (new Sql.Select())
      .with(withQuantB)
      .select(
        ['quant_b.labels', 'labels'],
        [new Sql.Raw(`intDiv(quant_b.timestamp_ns, ${stepMS}) * ${stepMS}`), 'timestamp_ns'],
        [new Sql.Raw('argMin(quant_b.value, quant_b.timestamp_ns)'), 'value'])
      .from(new Sql.WithReference(withQuantB))
      .groupBy('labels', 'timestamp_ns')
      .orderBy('labels', 'timestamp_ns')
  }
}
