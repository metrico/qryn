const Sql = require('@cloki/clickhouse-sql')
const { standardBuilder } = require('./shared')
/**
 * @type {ProcessFn}
 */
const processFn = (sel, ctx) => {
  const table = !ctx.isCluster ? ctx.tracesTable : ctx.tracesDistTable
  const withMain = new Sql.With('index_grouped', sel)
  const withTraceIds = new Sql.With('trace_ids', (new Sql.Select())
    .select('trace_id', 'span_id')
    .from(new Sql.WithReference(withMain))
    .join('span_id', 'array'))
  return (new Sql.Select())
    .with(withMain, withTraceIds)
    .select(
      [new Sql.Raw('lower(hex(traces.trace_id))'), 'trace_id'],
      [new Sql.Raw('arrayMap(x -> lower(hex(x)), groupArray(traces.span_id))'), 'span_id'],
      [new Sql.Raw('groupArray(traces.duration_ns)'), 'duration'],
      [new Sql.Raw('groupArray(traces.timestamp_ns)'), 'timestamp_ns'],
      [new Sql.Raw('min(traces.timestamp_ns)'), 'start_time_unix_nano'],
      [new Sql.Raw(
        'toFloat64(max(traces.timestamp_ns + traces.duration_ns) - min(traces.timestamp_ns)) / 1000000'
      ), 'duration_ms'],
      [new Sql.Raw('argMin(traces.name, traces.timestamp_ns)', 'root_service_name'), 'root_service_name']
    ).from([table, 'traces']).where(Sql.And(
      new Sql.In(new Sql.Raw('(traces.trace_id, traces.span_id)'), 'in', new Sql.WithReference(withTraceIds))
    )).groupBy('traces.trace_id')
    .orderBy(['start_time_unix_nano', 'desc'])
}

module.exports = standardBuilder(processFn)
