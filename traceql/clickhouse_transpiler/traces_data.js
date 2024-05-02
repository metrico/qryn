const Sql = require('@cloki/clickhouse-sql')
const { standardBuilder } = require('./shared')
/**
 * @type {ProcessFn}
 */
const processFn = (sel, ctx) => {
  const table = !ctx.isCluster ? ctx.tracesTable : ctx.tracesDistTable
  const withMain = new Sql.With('index_grouped', sel)
  const withTraceIds = new Sql.With('trace_ids', (new Sql.Select())
    .select('trace_id')
    .from(new Sql.WithReference(withMain)))
  return (new Sql.Select())
    .with(withMain, withTraceIds)
    .select(
      [new Sql.Raw('lower(hex(traces.trace_id))'), 'trace_id'],
      [new Sql.Raw('arrayMap(x -> lower(hex(x)), any(index_grouped.span_id))'), 'span_id'],
      [new Sql.Raw('any(index_grouped.duration)'), 'duration'],
      [new Sql.Raw('any(index_grouped.timestamp_ns)'), 'timestamp_ns'],
      [new Sql.Raw('min(traces.timestamp_ns)'), 'start_time_unix_nano'],
      [new Sql.Raw(
        'toFloat64(max(traces.timestamp_ns + traces.duration_ns) - min(traces.timestamp_ns)) / 1000000'
      ), 'duration_ms'],
      [new Sql.Raw('argMin(traces.name, traces.timestamp_ns)', 'root_service_name'), 'root_service_name']
    ).from([table, 'traces']).join(
      new Sql.WithReference(withMain),
      'left any',
      Sql.Eq(new Sql.Raw('traces.trace_id'), new Sql.Raw('index_grouped.trace_id'))
    ).where(Sql.And(
      new Sql.In(new Sql.Raw('traces.trace_id'), 'in', new Sql.WithReference(withTraceIds))
    )).groupBy('traces.trace_id')
    .orderBy(['start_time_unix_nano', 'desc'])
}

module.exports = standardBuilder(processFn)
