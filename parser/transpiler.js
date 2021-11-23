const streamSelectorOperatorRegistry = require('./registry/stream_selector_operator_registry')
const lineFilterOperatorRegistry = require('./registry/line_filter_operator_registry')
const logRangeAggregationRegistry = require('./registry/log_range_aggregation_registry')
const highLevelAggregationRegistry = require('./registry/high_level_aggregation_registry')
const numberOperatorRegistry = require('./registry/number_operator_registry')
const complexLabelFilterRegistry = require('./registry/complex_label_filter_expression')
const lineFormat = require('./registry/line_format')
const parserRegistry = require('./registry/parser_registry')
const unwrap = require('./registry/unwrap')
const unwrapRegistry = require('./registry/unwrap_registry')
const { _and, durationToMs } = require('./registry/common')
const compiler = require('./bnf')
const { parseMs, DATABASE_NAME, samplesReadTableName, isClustered } = require('../lib/utils')
const { getPlg } = require('../plugins/engine')

/**
 * @param options {{samplesTable?: string} | undefined}
 * @returns {registry_types.Request}
 */
module.exports.initQuery = (options) => {
  options = options || {}
  const samplesTable = options.samplesTable || samplesReadTableName
  return {
    select: ['time_series.labels as labels', 'samples.string as string', 'samples.fingerprint as fingerprint',
      'samples.timestamp_ms as timestamp_ms'],
    from: `${DATABASE_NAME()}.${samplesTable} as samples`,
    left_join: [{
      name: `${DATABASE_NAME()}.time_series`,
      on: ['AND', 'samples.fingerprint = time_series.fingerprint']
    }],
    limit: 1000,
    order_by: {
      name: ['timestamp_ms', 'labels'],
      order: 'desc'
    }
  }
}

/**
 *
 * @param request {{query: string, limit: number, direction: string, start: string, end: string, step: string,
 *      stream?: (function(DataStream): DataStream)[]}}
 * @returns {{query: string, matrix: boolean, duration: number | undefined}}
 */
module.exports.transpile = (request) => {
  const expression = compiler.ParseScript(request.query.trim())
  const token = expression.rootToken
  if (token.Child('user_macro')) {
    return module.exports.transpile({
      ...request,
      query: module.exports.transpileMacro(token.Child('user_macro'))
    })
  }

  let start = parseMs(request.start, Date.now() - 3600 * 1000)
  let end = parseMs(request.end, Date.now())
  const step = request.step ? parseInt(request.step) * 1000 : 0
  let query = module.exports.initQuery()
  if (request.limit) {
    query.limit = request.limit
  }
  query.order_by.order = request.direction === 'forward' ? 'asc' : 'desc'
  if (token.Child('aggregation_operator')) {
    const duration = durationToMs(token.Child('duration_value').value)
    start = Math.floor(start / duration) * duration
    end = Math.ceil(end / duration) * duration
    query.ctx = {
      start: start,
      end: end
    }
    query = _and(query, [
      `timestamp_ms >= ${start}`,
      `timestamp_ms <= ${end}`
    ])
    query = module.exports.transpileAggregationOperator(token, query)
  } else if (token.Child('unwrap_function')) {
    const duration = durationToMs(token.Child('unwrap_function').Child('duration_value').value)
    start = Math.floor(start / duration) * duration
    end = Math.ceil(end / duration) * duration
    query.ctx = {
      start: start,
      end: end,
      step: step
    }
    query = _and(query, [
      `timestamp_ms >= ${start}`,
      `timestamp_ms <= ${end}`
    ])
    query = module.exports.transpileUnwrapFunction(token, query)
  } else if (token.Child('log_range_aggregation')) {
    const duration = durationToMs(token.Child('log_range_aggregation').Child('duration_value').value)
    start = Math.floor(start / duration) * duration
    end = Math.ceil(end / duration) * duration
    query.ctx = {
      start: start,
      end: end,
      step: step
    }
    query = _and(query, [
      `timestamp_ms >= ${start}`,
      `timestamp_ms <= ${end}`
    ])
    query = module.exports.transpileLogRangeAggregation(token, query)
  } else {
    query = module.exports.transpileLogStreamSelector(token, query)
    query = _and(query, [
      `timestamp_ms >= ${start}`,
      `timestamp_ms <= ${end}`
    ])
    query = {
      ctx: query.ctx,
      stream: query.stream,
      with: {
        ...query.with || {},
        sel_a: {
          ...query,
          ctx: undefined,
          with: undefined,
          stream: undefined
        }
      },
      select: ['*'],
      from: 'sel_a',
      order_by: {
        name: ['labels', 'timestamp_ms'],
        order: query.order_by.order
      }
    }
  }
  if (token.Child('compared_agg_statement')) {
    const op = token.Child('compared_agg_statement_cmp').Child('number_operator').value
    query = numberOperatorRegistry[op](token.Child('compared_agg_statement'), query)
  }

  return {
    query: module.exports.requestToStr(query),
    matrix: !!query.matrix,
    duration: query.ctx && query.ctx.duration ? query.ctx.duration : 1000,
    stream: query.stream
  }
}

/**
 *
 * @param request {{query: string, stream?: (function(DataStream): DataStream)[], samplesTable?: string}}
 * @returns {{query: string, stream: (function(DataStream): DataStream)[]}}
 */
module.exports.transpileTail = (request) => {
  const expression = compiler.ParseScript(request.query.trim())
  const denied = ['user_macro', 'aggregation_operator', 'unwrap_function', 'log_range_aggregation']
  for (const d of denied) {
    if (expression.rootToken.Child(d)) {
      throw new Error(`${d} is not supported. Only raw logs are supported`)
    }
  }
  let query = module.exports.initQuery({ samplesTable: request.samplesTable })
  query = _and(query, [
    'timestamp_ms >= (toUnixTimestamp(now()) - 5) * 1000'
  ])
  query = module.exports.transpileLogStreamSelector(expression.rootToken, query)
  query.order_by = {
    name: ['timestamp_ms'],
    order: 'ASC'
  }
  query.limit = undefined
  return {
    query: module.exports.requestToStr(query),
    stream: query.stream || []
  }
}

/**
 *
 * @param request {string[]} ['{ts1="a1"}', '{ts2="a2"}', ...]
 * @returns {string} clickhouse query
 */
module.exports.transpileSeries = (request) => {
  if (request.length === 0) {
    return ''
  }
  /**
   *
   * @param req {string}
   * @returns {registry_types.Request}
   */
  const getQuery = (req) => {
    const expression = compiler.ParseScript(req.trim())
    const query = module.exports.transpileLogStreamSelector(expression.rootToken, module.exports.initQuery())
    return {
      ...query.with.str_sel,
      select: ['labels']
    }
  }
  if (request.length === 1) {
    return module.exports.requestToStr({
      ...getQuery(request[0]),
      distinct: true
    })
  }
  const queries = request.map(getQuery)
  return module.exports.requestToStr({
    ...queries[0],
    distinct: true,
    where: ['OR', ...queries.map(q => q.where)]
  })
}

/**
 *
 * @param token {Token}
 * @returns {string}
 */
module.exports.transpileMacro = (token) => {
  const plg = Object.values(getPlg({ type: 'macros' })).find(m => token.Child(m._main_rule_name))
  return plg.stringify(token)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpileAggregationOperator = (token, query) => {
  const agg = token.Child('aggregation_operator')
  if (token.Child('log_range_aggregation')) {
    query = module.exports.transpileLogRangeAggregation(agg, query)
  } else if (token.Child('unwrap_function')) {
    query = module.exports.transpileUnwrapFunction(agg, query)
  }
  return highLevelAggregationRegistry[agg.Child('aggregation_operator_fn').value](token, query)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpileLogRangeAggregation = (token, query) => {
  const agg = token.Child('log_range_aggregation')
  query = module.exports.transpileLogStreamSelector(agg, query)
  return logRangeAggregationRegistry[agg.Child('log_range_aggregation_fn').value](token, query)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpileLogStreamSelector = (token, query) => {
  const rules = token.Children('log_stream_selector_rule')
  for (const rule of rules) {
    const op = rule.Child('operator').value
    query = streamSelectorOperatorRegistry[op](rule, query)
  }
  for (const pipeline of token.Children('log_pipeline')) {
    if (pipeline.Child('line_filter_expression')) {
      const op = pipeline.Child('line_filter_operator').value
      query = lineFilterOperatorRegistry[op](pipeline, query)
      continue
    }
    if (pipeline.Child('parser_expression')) {
      const op = pipeline.Child('parser_fn_name').value
      query = parserRegistry[op](pipeline, query)
      continue
    }
    if (pipeline.Child('label_filter_pipeline')) {
      query = module.exports.transpileLabelFilterPipeline(pipeline.Child('label_filter_pipeline'), query)
      continue
    }
    if (pipeline.Child('line_format_expression')) {
      query = lineFormat(pipeline, query)
      continue
    }
  }
  for (const c of ['labels_format_expression']) {
    if (token.Children(c).length > 0) {
      throw new Error(`${c} not supported`)
    }
  }
  return query
}

/**
 *
 * @param pipeline {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpileLabelFilterPipeline = (pipeline, query) => {
  return complexLabelFilterRegistry(pipeline.Child('complex_label_filter_expression'), query)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpileUnwrapFunction = (token, query) => {
  query = module.exports.transpileUnwrapExpression(token.Child('unwrap_expression'), query)
  return unwrapRegistry[token.Child('unwrap_fn').value](token, query)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpileUnwrapExpression = (token, query) => {
  query = module.exports.transpileLogStreamSelector(token, query)
  return unwrap(token.Child('unwrap_statement'), query)
}

/**
 *
 * @param query {registry_types.Request | registry_types.UnionRequest}
 * @returns {string}
 */
module.exports.requestToStr = (query) => {
  if (query.requests) {
    return query.requests.map(r => `(${module.exports.requestToStr(r)})`).join(' UNION ALL ')
  }
  let req = query.with
    ? 'WITH ' + Object.entries(query.with).filter(e => e[1])
      .map(e => `${e[0]} as (${module.exports.requestToStr(e[1])})`).join(', ')
    : ''
  req += ` SELECT ${query.distinct ? 'DISTINCT' : ''} ${query.select.join(', ')} FROM ${query.from} `
  for (const clause of query.left_join || []) {
    req += ` ${isClustered ? 'GLOBAL ' : ''}LEFT JOIN ${clause.name} ON ${whereBuilder(clause.on)}`
  }
  req += query.where && query.where.length ? ` WHERE ${whereBuilder(query.where)} ` : ''
  req += query.group_by ? ` GROUP BY ${query.group_by.join(', ')}` : ''
  req += query.having && query.having.length ? ` HAVING ${whereBuilder(query.having)}` : ''
  req += query.order_by ? ` ORDER BY ${query.order_by.name.map(n => n + ' ' + query.order_by.order).join(', ')} ` : ''
  req += typeof (query.limit) !== 'undefined' ? ` LIMIT ${query.limit}` : ''
  req += typeof (query.offset) !== 'undefined' ? ` OFFSET ${query.offset}` : ''
  req += query.final ? ' FINAL' : ''
  return req
}

/**
 *
 * @param clause {(string | string[])[]}
 */
const whereBuilder = (clause) => {
  const op = clause[0]
  const _clause = clause.slice(1).map(c => Array.isArray(c) ? `(${whereBuilder(c)})` : c)
  return _clause.join(` ${op} `)
}
