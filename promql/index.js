const Sql = require('@cloki/clickhouse-sql')
const prometheus = require('../wasm_parts/main')
const { rawRequest } = require('../lib/db/clickhouse')
const { DATABASE_NAME } = require('../lib/utils')
const { clusterName, metricType, bothType } = require('../common')
const _dist = clusterName ? '_dist' : ''

class PSQLError extends Error {}
module.exports.PSQLError = PSQLError

/**
 *
 * @param query {string}
 * @param startMs {number}
 * @param endMs {number}
 * @param stepMs {number}
 */
module.exports.rangeQuery = async (query, startMs, endMs, stepMs) => {
  let resp
  try {
    resp = await prometheus.pqlRangeQuery(query, startMs, endMs, stepMs, module.exports.getData)
    return JSON.parse(resp)
  } catch (e) {
    if (e instanceof prometheus.WasmError) {
      throw new PSQLError(e.message)
    }
    throw e
  }
}

module.exports.instantQuery = async (query, timeMs) => {
  try {
    const resp = await prometheus.pqlInstantQuery(query, timeMs, module.exports.getData)
    return JSON.parse(resp)
  } catch (e) {
    if (e instanceof prometheus.WasmError) {
      throw new PSQLError(e.message)
    }
    throw e
  }
}

module.exports.series = async (query, fromMs, toMs) => {
  try {
    const fromS = Math.floor(fromMs / 1000)
    const toS = Math.floor(toMs / 1000)
    const matchers = prometheus.pqlMatchers(query)
    const conds = getMatchersIdxCond(matchers[0])
    const idx = getIdxSubquery(conds, fromMs, toMs)
    const withIdx = new Sql.With('idx', idx, !!clusterName)
    const req = (new Sql.Select())
      .with(withIdx)
      .select([new Sql.Raw('any(labels)'), 'labels'])
      .from(`time_series${_dist}`)
      .where(Sql.And(
        Sql.Gte('date', new Sql.Raw(`toDate(fromUnixTimestamp(${fromS}))`)),
        Sql.Lte('date', new Sql.Raw(`toDate(fromUnixTimestamp(${toS}))`)),
        new Sql.In('fingerprint', 'in', new Sql.WithReference(withIdx)),
        new Sql.In('type', 'in', [bothType,metricType])))
      .groupBy(new Sql.Raw('fingerprint'))
    const data = await rawRequest(req.toString() + ' FORMAT JSON',
      null,
      DATABASE_NAME())
    return data.data.data.map(l => JSON.parse(l.labels))
  } catch (e) {
    if (e instanceof prometheus.WasmError) {
      throw new PSQLError(e.message)
    }
    throw e
  }
}

/**
 *
 * @param matchers {[[string]]}
 */
const getMatchersIdxCond = (matchers) => {
  const matchesCond = []
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
        _matcher.push(Sql.Ne(new Sql.Raw(`match(val, ${Sql.quoteVal(matcher[2])})`), 1))
    }
    matchesCond.push(Sql.And(..._matcher))
  }
  return matchesCond
}

const getIdxSubquery = (conds, fromMs, toMs) => {
  const fromS = Math.floor(fromMs / 1000)
  const toS = Math.floor(toMs / 1000)
  return (new Sql.Select())
    .select('fingerprint')
    .from([DATABASE_NAME() + '.time_series_gin', 'time_series_gin'])
    .where(Sql.And(
      Sql.Or(...conds),
      Sql.Gte('date', new Sql.Raw(`toDate(fromUnixTimestamp(${fromS}))`)),
      Sql.Lte('date', new Sql.Raw(`toDate(fromUnixTimestamp(${toS}))`)),
      new Sql.In('type', 'in', [bothType, metricType])))
    .having(
      Sql.Eq(
        new Sql.Raw('groupBitOr(' + conds.map(
          (m, i) => new Sql.Raw(`bitShiftLeft((${m})::UInt64, ${i})`)
        ).join('+') + ')'), (1 << conds.length) - 1)
    ).groupBy('fingerprint')
}

module.exports.getData = async (matchers, fromMs, toMs, subqueries) => {
  const db = DATABASE_NAME()
  const subq = (subqueries || {})[getMetricName(matchers)]
  if (subq) {
    const data = await rawRequest(subq + ' FORMAT RowBinary',
      null, db, { responseType: 'arraybuffer' })
    return new Uint8Array(data.data)
  }
  const matches = getMatchersIdxCond(matchers)
  const idx = getIdxSubquery(matches, fromMs, toMs)
  const withIdx = new Sql.With('idx', idx, !!clusterName)
  const timeSeries = (new Sql.Select())
    .select(
      'fingerprint',
      [new Sql.Raw('arraySort(JSONExtractKeysAndValues(labels, \'String\'))'), 'labels']
    ).from(DATABASE_NAME() + '.time_series')
    .where(Sql.And(
      new Sql.In('fingerprint', 'in', new Sql.WithReference(withIdx)),
      new Sql.In('type', 'in', [bothType,metricType])))
  const withTimeSeries = new Sql.With('timeSeries', timeSeries, !!clusterName)
  const raw = (new Sql.Select())
    .with(withIdx)
    .select(
      [new Sql.Raw('argMaxMerge(last)'), 'value'],
      'fingerprint',
      [new Sql.Raw('intDiv(timestamp_ns, 15000000000) * 15000'), 'timestamp_ms'])
    .from([`metrics_15s${_dist}`, 'metrics_15s'])
    .where(
      new Sql.And(
        new Sql.In('fingerprint', 'in', new Sql.WithReference(withIdx)),
        Sql.Gte('timestamp_ns', new Sql.Raw(`${fromMs}000000`)),
        Sql.Lte('timestamp_ns', new Sql.Raw(`${toMs}000000`)),
        new Sql.In('type', 'in', [bothType, metricType]))
    ).groupBy('fingerprint', 'timestamp_ms')
    .orderBy('fingerprint', 'timestamp_ms')
  if (clusterName) {
    raw.select([new Sql.Raw('min(time_series.labels)'), 'labels']).join(
      [new Sql.WithReference(withTimeSeries), 'time_series'],
      'any left',
      Sql.Eq('time_series.fingerprint', new Sql.Raw('metrics_15s.fingerprint'))
    )
  }
  const withRaw = new Sql.With('raw', raw, !!clusterName)
  const res = (new Sql.Select())
    .with(withRaw)
    .select(
      [new Sql.Raw('any(labels)'), 'stream'],
      [new Sql.Raw('arraySort(groupArray((raw.timestamp_ms, raw.value)))'), 'values']
    ).from([new Sql.WithReference(withRaw), 'raw'])
    .groupBy('raw.fingerprint')
    .orderBy('raw.fingerprint')
  if (!clusterName) {
    res.with(withTimeSeries)
      .join(
        [new Sql.WithReference(withTimeSeries), 'time_series'],
        'any left',
        Sql.Eq('time_series.fingerprint', new Sql.Raw('raw.fingerprint'))
      )
  }
  console.log(res.toString())
  const data = await rawRequest(res.toString() + ' FORMAT RowBinary',
    null, db, { responseType: 'arraybuffer' })
  return new Uint8Array(data.data)
}

function getMetricName(matchers) {
  for (const matcher of matchers) {
    if (matcher[0] === '__name__' && matcher[1] === '=') {
      return matcher[2]
    }
  }
}

prometheus.getData = module.exports.getData
