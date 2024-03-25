const parser = require('./parser')
const transpiler = require('./clickhouse_transpiler')
const logger = require('../lib/logger')
const { DATABASE_NAME } = require('../lib/utils')
const { clusterName } = require('../common')
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
  const _dbname = DATABASE_NAME()
  /** @type {Context} */
  const ctx = {
    tracesDistTable: `${_dbname}.tempo_traces_dist`,
    tracesTable: `${_dbname}.tempo_traces`,
    isCluster: !!clusterName,
    tracesAttrsTable: `${_dbname}.tempo_traces_attrs_gin`,
    from: from,
    to: to,
    limit: limit
  }
  const scrpit = parser.ParseScript(query)
  const planner = transpiler(scrpit.rootToken)
  const sql = planner(ctx)
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
          startTimeUnixNano: row.timestamp_ns[i],
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
