const parser = require('./parser')
const { Planner } = require('./clickhouse_transpiler')
const logger = require('../lib/logger')
const { DATABASE_NAME } = require('../lib/utils')
const { clusterName } = require('../common')
const { rawRequest } = require('../lib/db/clickhouse')
const { postProcess } = require('./post_processor')

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
  const scrpit = parser.ParseScript(query)
  /** @type {Context} */
  const ctx = {
    tracesDistTable: `${_dbname}.tempo_traces_dist`,
    tracesTable: `${_dbname}.tempo_traces`,
    isCluster: !!clusterName,
    tracesAttrsTable: `${_dbname}.tempo_traces_attrs_gin`,
    from: from,
    to: to,
    limit: limit,
    randomFilter: null,
    planner: new Planner(scrpit.rootToken)
  }

  let complexity = await evaluateComplexity(ctx, scrpit.rootToken)
  let res = []
  if (complexity > 10000000) {
    complexity = ctx.planner.minify()
  }
  if (complexity > 10000000) {
    res = await processComplexResult(ctx, scrpit.rootToken, complexity)
  } else {
    res = await processSmallResult(ctx, scrpit.rootToken)
  }
  res = postProcess(res, scrpit.rootToken)
  res.forEach(t =>
    t.spanSets.forEach(
      ss => ss.spans.sort(
        (a, b) => b.startTimeUnixNano.localeCompare(a.startTimeUnixNano))
    )
  )
  console.log(JSON.stringify(res, 2))
  return res
}

/**
 *
 * @param ctx {Context}
 * @param script {Token}
 */
const evaluateComplexity = async (ctx, script) => {
  const evaluator = ctx.planner.planEval()
  const sql = evaluator(ctx)
  const response = await rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
  ctx.planner.setEvaluationResult(response.data.data)
  return response.data.data.reduce((acc, row) => Math.max(acc, row.count), 0)
}

/**
 *
 * @param ctx {Context}
 * @param script {Token}
 * @param complexity {number}
 */
async function processComplexResult (ctx, script, complexity) {
  const planner = ctx.planner.plan()
  const maxFilter = Math.floor(complexity / 10000000)
  //let traces = []
  let response = null
  for (let i = 0; i < maxFilter; i++) {
    ctx.randomFilter = [maxFilter, i]
    const sql = planner(ctx)
    response = await rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
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
    /*traces = response.data.data.map(row => ({
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
    }))*/
  }
  return response.data.data
}

/**
 *
 * @param ctx {Context}
 * @param script {Token}
 */
async function processSmallResult (ctx, script) {
  const planner = ctx.planner.plan()
  const sql = planner(ctx)
  const response = await rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
  /*const traces = response.data.data.map(row => ({
    traceID: row.trace_id,
    rootServiceName: row.root_service_name,
    rootTraceName: row.root_trace_name,
    startTimeUnixNano: row.start_time_unix_nano,
    durationMs: row.duration_ms,
    spanSet: {
      spans: row.span_id.map((spanId, i) => ({
        spanID: spanId,
        startTimeUnixNano: row.timestamp_ns[i],
        spanStartTime: row.timestamp_ns[i],
        durationNanos: row.duration[i],
        attributes: []
      })),
      matched: row.span_id.length
    },
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
  }))*/
  return response.data.data
}

module.exports = {
  search
}
