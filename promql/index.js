const Sql = require('@cloki/clickhouse-sql')
const prometheus = require('../wasm_parts/main')
const { rawRequest } = require('../lib/db/clickhouse')
const { DATABASE_NAME } = require('../lib/utils')
const { clusterName } = require('../common')
const _dist = clusterName ? '_dist' : ''
/**
 *
 * @param query {string}
 * @param startMs {number}
 * @param endMs {number}
 * @param stepMs {number}
 */
module.exports.rangeQuery = async (query, startMs, endMs, stepMs) => {
  const resp = await prometheus.pqlRangeQuery(query, startMs, endMs, stepMs, module.exports.getData)
  return JSON.parse(resp)
}

module.exports.instantQuery = async (query, timeMs) => {
  const resp = await prometheus.pqlInstantQuery(query, timeMs, module.exports.getData)
  return JSON.parse(resp)
}

module.exports.getData = async (matchers, fromMs, toMs) => {
  const matches = []
  for (const matcher of matchers) {
    const _matcher = [
      Sql.Eq('key', matcher[0])
    ]
    switch (matcher[1]) {
      case '=':
        _matcher.push(Sql.Eq('val', matcher[2]))
        break
      case '!=':
        _matcher.push(Sql.Ne('val', matcher[2]))
        break
      case '=~':
        _matcher.push(Sql.Eq(new Sql.Raw(`match(val, ${Sql.quoteVal(matcher[2])})`), 1))
        break
      case '!~':
        _matcher.push(Sql.Ne(Sql.Raw(`match(val, ${Sql.quoteVal(matcher[2])})`), 1))
    }
    matches.push(Sql.And(..._matcher))
  }

  const idx = (new Sql.Select())
    .select('fingerprint')
    .from('time_series_gin')
    .where(Sql.Or(...matches))
    .having(
      Sql.Eq(
        new Sql.Raw('groupBitOr(' + matches.map(
          (m, i) => new Sql.Raw(`bitShiftLeft((${m})::UInt64, ${i})`)
        ).join('+') + ')'), (1 << matches.length) - 1)
    ).groupBy('fingerprint')
  const withIdx = new Sql.With('idx', idx, false)
  const raw = (new Sql.Select())
    .with(withIdx)
    .select(
      [new Sql.Raw('argMaxMerge(last)'), 'value'],
      'fingerprint',
      [new Sql.Raw('intDiv(timestamp_ns, 15000000000) * 15000'), 'timestamp_ms'])
    .from('metrics_15s')
    .where(
      new Sql.And(
        new Sql.In('fingerprint', 'in', new Sql.WithReference(withIdx)),
        Sql.Gte('timestamp_ns', new Sql.Raw(`${fromMs}000000`)),
        Sql.Lte('timestamp_ns', new Sql.Raw(`${toMs}000000`))
      )
    ).groupBy('fingerprint', 'timestamp_ms')
    .orderBy('fingerprint', 'timestamp_ms')
  const timeSeries = (new Sql.Select())
    .select(
      'fingerprint',
      [new Sql.Raw('arraySort(JSONExtractKeysAndValues(labels, \'String\'))'), 'labels']
    ).from('time_series')
    .where(new Sql.In('fingerprint', 'in', new Sql.WithReference(withIdx)))
  const withRaw = new Sql.With('raw', raw, false)
  const withTimeSeries = new Sql.With('timeSeries', timeSeries, false)
  const res = (new Sql.Select())
    .with(withRaw, withTimeSeries)
    .select(
      [new Sql.Raw('any(labels)'), 'stream'],
      [new Sql.Raw('arraySort(groupArray((raw.timestamp_ms, raw.value)))'), 'values']
    ).from([new Sql.WithReference(withRaw), 'raw'])
    .join(
      [new Sql.WithReference(withTimeSeries), 'time_series'],
      'any left',
      Sql.Eq('time_series.fingerprint', new Sql.Raw('raw.fingerprint'))
    ).groupBy('raw.fingerprint')
    .orderBy('raw.fingerprint')

  const db = DATABASE_NAME()
  console.log('!!!!!!!!!!! ' + res.toString())
  const data = await rawRequest(res.toString() + ' FORMAT RowBinary',
    null, db, { responseType: 'arraybuffer' })
  return new Uint8Array(data.data)
}

prometheus.getData = module.exports.getData
