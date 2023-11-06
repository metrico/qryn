const { TranspileTraceQL } = require('../wasm_parts/main')
const { clusterName } = require('../common')
const { DATABASE_NAME } = require('../lib/utils')
const dist = clusterName ? '_dist' : ''
const { rawRequest } = require('../lib/db/clickhouse')

/**
 *
 * @param query {string}
 * @param limit {number}
 * @param from {Date}
 * @param to {Date}
 * @returns {Promise<[]>}
 */
const search = async (query, limit, from, to) => {
  const request = {
    Request: query,
    Ctx: {
      IsCluster: !!clusterName,
      OrgID: '0',
      FromS: Math.floor(from.getTime() / 1000) - 600,
      ToS: Math.floor(to.getTime() / 1000),
      Limit: parseInt(limit),

      TimeSeriesGinTableName: 'time_series_gin',
      SamplesTableName: `samples_v3${dist}`,
      TimeSeriesTableName: 'time_series',
      TimeSeriesDistTableName: 'time_series_dist',
      Metrics15sTableName: `metrics_15s${dist}`,

      TracesAttrsTable: 'tempo_traces_attrs_gin',
      TracesAttrsDistTable: 'tempo_traces_attrs_gin_dist',
      TracesTable: 'tempo_traces',
      TracesDistTable: 'tempo_traces_dist'
    }
  }
  console.log(JSON.stringify(request))
  const sql = TranspileTraceQL(request)
  const response = await rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
  const traces = response.data.data.map(row => ({
    traceID: row.trace_id,
    rootServiceName: row.root_service_name,
    rootTraceName: row.root_trace_name,
    startTimeUnixNano: row.start_time_unix_nano,
    durationMs: row.duration_ms,
    spanSets: [
      {
        spans: row.span_id.map((spanId, i) => ({
          spanID: spanId,
          startTimeUnixNano: row.timestamps_ns[i],
          durationNanos: row.duration[i],
          attributes: []
        })),
        matched: row.span_id.length
      }
    ]
  }))
  return traces
}

module.exports = {
  search
}
