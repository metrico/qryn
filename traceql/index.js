const parser = require('./parser')
const { transpile, evaluateCmpl } = require('./clickhouse_transpiler')
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
    limit: limit,
    randomFilter: null
  }
  const scrpit = parser.ParseScript(query)
  const complexity = await evaluateComplexity(ctx, scrpit.rootToken)
  if (complexity > 10000000) {
    return await processComplexResult(ctx, scrpit.rootToken, complexity)
  }
  return await processSmallResult(ctx, scrpit.rootToken)
}

/**
 *
 * @param ctx {Context}
 * @param script {Token}
 */
const evaluateComplexity = async (ctx, script) => {
  const evaluator = evaluateCmpl(script)
  const sql = evaluator(ctx)
  const response = await rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
  return response.data.data.reduce((acc, row) => Math.max(acc, row.count), 0)
}

/**
 *
 * @param ctx {Context}
 * @param script {Token}
 * @param complexity {number}
 */
async function processComplexResult (ctx, script, complexity) {
  const planner = transpile(script)
  const maxFilter = Math.floor(complexity / 10000000)
  let traces = []
  for (let i = 0; i < maxFilter; i++) {
    ctx.randomFilter = [maxFilter, i]
    let sql = planner(ctx)
    let response = await rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
    if (response.data.data.length === parseInt(ctx.limit)) {
      const minStart = response.data.data.reduce((acc, row) =>
        acc === 0 ? row.start_time_unix_nano : Math.min(acc, row.start_time_unix_nano), 0
      )
      ctx.from = new Date(Math.floor(minStart / 1000000))
      ctx.randomFilter = null
      complexity = await evaluateComplexity(ctx, script)
      if (complexity <= 10000000) {
        return await processSmallResult(ctx, script)
      }
      ctx.randomFilter = [maxFilter, i]
    }
    ctx.cachedTraceIds = response.data.data.map(row => row.trace_id)
    sql = planner(ctx)
    response = await rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
    traces = response.data.data.map(row => ({
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
  }
  return traces
}

/**
 *
 * @param ctx {Context}
 * @param script {Token}
 */
async function processSmallResult (ctx, script) {
  const planner = transpile(script)
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
