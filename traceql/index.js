const { TranspileTraceQL } = require('../wasm_parts/main')
const { clusterName } = require('../common')
const { DATABASE_NAME } = require('../lib/utils')
const dist = clusterName ? '_dist' : ''
const { rawRequest } = require('../lib/db/clickhouse')
const logger = require('../lib/logger')

/**
 *
 * @param query {string}
 * @param limit {number}
 * @param from {Date}
 * @param to {Date}
 * @returns {Promise<[]>}
 */
const search = async (query, limit, from, to) => {
  const _dbname = '`' + DATABASE_NAME() + '`'
  const request = {
    Request: query,
    Ctx: {
      IsCluster: !!clusterName,
      OrgID: '0',
      FromS: Math.floor(from.getTime() / 1000) - 600,
      ToS: Math.floor(to.getTime() / 1000),
      Limit: parseInt(limit),

      TimeSeriesGinTableName: `${_dbname}.time_series_gin`,
      SamplesTableName: `${_dbname}.samples_v3${dist}`,
      TimeSeriesTableName: `${_dbname}.time_series`,
      TimeSeriesDistTableName: `${_dbname}.time_series_dist`,
      Metrics15sTableName: `${_dbname}.metrics_15s${dist}`,

      TracesAttrsTable: `${_dbname}.tempo_traces_attrs_gin`,
      TracesAttrsDistTable: `${_dbname}.tempo_traces_attrs_gin_dist`,
      TracesTable: `${_dbname}.tempo_traces`,
      TracesDistTable: `${_dbname}.tempo_traces_dist`
    }
  }
  logger.debug(JSON.stringify(request))
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
