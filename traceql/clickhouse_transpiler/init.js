const Sql = require('@cloki/clickhouse-sql')
const { format } = require('date-fns')
/**
 * @typedef {function(Sql.Select, {
 *   from: Date,
 *   to: Date,
 *   tracesAttrsTable: string,
 *   limit: number
 * }): Select} ProcessFn
 * @type ProcessFn
 */
module.exports.process = (sel, ctx) => {
  return (new Sql.Select()).select(['trace_id', 'trace_id'],
    [new Sql.Raw('lower(hex(span_id))'), 'span_id'],
    [new Sql.Raw('any(duration)'), 'duration'],
    [new Sql.Raw('any(timestamp_ns)', 'timestamp_ns')])
    .from([ctx.tracesAttrsTable, 'traces_idx'])
    .where(Sql.And(
      Sql.Gte('date', Sql.val(format(ctx.from, 'yyyy-MM-dd'))),
      Sql.Lt('date', Sql.val(format(ctx.to, 'yyyy-MM-dd'))),
      Sql.Gte('traces_idx.timestamp_ns', new Sql.Raw(ctx.from.getTime() + '000000')),
      Sql.Lt('traces_idx.timestamp_ns', new Sql.Raw(ctx.to.getTime() + '000000'))
    )).groupBy('trace_id', 'span_id')
    .orderBy(['timestamp_ns', 'desc'])
}
