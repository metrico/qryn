const Sql = require('@cloki/clickhouse-sql')
const { standardBuilder } = require('./shared')

module.exports = standardBuilder((sel, ctx) => {
  const withMain = new Sql.With('index_search', sel)
  return (new Sql.Select())
    .with(withMain)
    .select(
      ['trace_id', 'trace_id'],
      [new Sql.Raw('groupArray(100)(span_id)'), 'span_id']
    ).from(new Sql.WithReference(withMain))
    .groupBy('trace_id')
    .orderBy([new Sql.Raw('max(index_search.timestamp_ns)'), 'desc'])
})
