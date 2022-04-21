const { DATABASE_NAME } = require('../../lib/utils')
const Sql = require('@cloki/clickhouse-sql')
const lineFilterOperatorRegistry = require('../registry/line_filter_operator_registry')
const logRangeAggregationRegistry = require('../registry/log_range_aggregation_registry')
const aggregationOperatorRegistry = require('../registry/high_level_aggregation_registry')
const unwrapRegistry = require('../registry/unwrap_registry')
const parserRegistry = require('../registry/parser_registry')
const { getStream } = require('../registry/common')

/**
 * @param root {Token}
 * @param request {{
 * query: string,
 * limit: number,
 * direction: string,
 * start: string,
 * end: string,
 * step: string,
 * stream?: (function(DataStream): DataStream)[],
 * rawQuery: boolean
 * }}
 * @returns {{query: string, stream: (function (DataStream): DataStream)[], matrix: boolean, duration: number | undefined}}
 */
module.exports.transpile = (root, request) => {
  const db = getLabelValue('--db', root) || DATABASE_NAME()
  const table = getLabelValue('--table', root)
  const tsNS = getLabelValue('--timestampNs', root)
  const msg = getLabelValue('--message', root)
  const val = getLabelValue('--value', root)
  for (const req of [[table, '--table'], [tsNS, '--timestampNs']]) {
    if (!req[0]) {
      throw new Error(`"${req[1]}" tag is required`)
    }
  }
  if (!msg && !val) {
    throw new Error('--message or --value tag is required')
  }
  if (val && !root.Child('unwrap_value_statement')) {
    throw new Error('--value tag should have unwrap_value pipeline')
  }
  const tsMs = val || root.Child('log_range_aggregation')
  const lbls = new Labels()
  const tsNsExp = `toInt64(toFloat64(${tsNS}) * pow(10, 18 - toUInt8(log10(toFloat64(${tsNS})))))`
  let query = (new Sql.Select())
    .select(
      (msg ? [new Sql.Raw(msg), 'string'] : null),
      (val ? [new Sql.Raw(val), 'value'] : null),
      [new Sql.Raw('1'), 'fingerprint'],
      (tsMs ? [new Sql.Raw(`intDiv(${tsNsExp}, 1000000)`), 'timestamp_ns'] : [new Sql.Raw(tsNsExp), 'timestamp_ns']),
      [lbls, 'labels']
    ).from([`${db}.${table}`, 'samples'])
    .where(Sql.And(Sql.Lt('samples.timestamp_ns', request.end), Sql.Gte('samples.timestamp_ns', request.start)))
    .limit(msg ? parseInt(request.limit) : 0)
  query.select_list = query.select_list.filter(s => s)
  if (!val) {
    query.orderBy(['timestamp_ns', 'desc'])
  } else {
    query.orderBy(['labels', 'asc'], ['timestamp_ns', 'asc'])
  }
  for (const pipeline of root.Children('log_pipeline')) {
    if (pipeline.Child('line_filter_expression')) {
      const op = pipeline.Child('line_filter_operator').value
      query = lineFilterOperatorRegistry[op](pipeline, query)
      continue
    } else if (pipeline.Child('parser_expression')) {
      const op = pipeline.Child('parser_fn_name').value
      query = parserRegistry[op](pipeline, query)
      continue
    } else if (pipeline.Child('labels_format_expression')) {
      for (const param of pipeline.Children('labels_format_expression_param')) {
        if (param.Child('label_inject_param')) {
          lbls.addLbl(param.Child('label').value, JSON.parse(param.Child('quoted_str').value))
        }
        if (param.Child('label_rename_param')) {
          const _lbls = param.Children('label')
          lbls.addLbl(_lbls[0].value, _lbls[1].value)
        }
      }
    } else if (pipeline.Child('label_filter_pipeline')) {
      let where = ''
      const marshal = (token) => {
        switch (token.name) {
          case 'label_filter_pipeline':
          case 'complex_label_filter_expression':
            token.tokens.forEach(marshal)
            break
          case 'bracketed_label_filter_expression':
            where += '('
            token.tokens.forEach(marshal)
            where += ')'
            break
          case 'and_or':
            where += ' ' + token.value + ' '
            break
          case 'label_filter_expression':
            token.tokens.forEach(marshal)
            break
          case 'string_label_filter_expression':
          case 'number_label_filter_expression':
            // eslint-disable-next-line no-case-declarations
            const lbl = token.Child('label').value
            // eslint-disable-next-line no-case-declarations
            const expr = lbls.getLbl(lbl)
            // eslint-disable-next-line no-case-declarations
            const val = token.name === 'number_label_filter_expression'
              ? token.Child('NUMBER').value
              : JSON.parse(token.Child('quoted_str').value)
            // eslint-disable-next-line no-case-declarations
            let op = token.Child(token.name === 'number_label_filter_expression'
              ? 'number_operator'
              : 'operator').value
            op = token.name === 'number_label_filter_expression' && op === '!=' ? '!==' : op
            if (expr) {
              where += labelFilterOps[op](expr, val)
            } else {
              if (query.select_list.some(f => f[1] === 'extra_labels')) {
                where += '(' +
                  `arrayExists(x -> x.1 == '${lbl}') AND ` +
                  labelFilterOps[lbl.Child('operator')](`arrayFirst(x -> x.1 == ${lbl}).2`, val) + ')'
              }
            }
        }
      }
      marshal(pipeline.Child('label_filter_pipeline'))
      const cond = new Sql.Condition()
      cond.toString = () => `(${where})`
      query.where(cond)
    } else {
      throw new Error('Not supported')
    }
  }
  let matrix = !!val
  let duration = 1
  query.ctx.step = request.step
  if (root.Child('log_range_aggregation')) {
    const op = root.Child('log_range_aggregation_fn').value
    if (!logRangeAggregationRegistry[op]) {
      throw new Error('Not supported')
    }
    query = logRangeAggregationRegistry[op](root.Child('log_range_aggregation'), query)
    matrix = true
    duration = query.ctx.duration
  }
  if (root.Child('unwrap_function')) {
    const op = root.Child('unwrap_fn').value
    if (!unwrapRegistry[op]) {
      throw new Error('Not supported')
    }
    query.select_list = query.select_list.map(l => l[1] === 'value' ? [l[0], 'unwrapped'] : l)
    query = unwrapRegistry[op](root.Child('unwrap_function'), query)
    matrix = true
    duration = query.ctx.duration
  }
  if (root.Child('aggregation_operator')) {
    const op = root.Child('aggregation_operator_fn').value
    if (!aggregationOperatorRegistry[op]) {
      throw new Error('Not supported')
    }
    query = aggregationOperatorRegistry[op](root.Child('aggregation_operator'), query)
    matrix = true
    duration = query.ctx.duration
  }
  console.log(query.toString())
  return {
    query: request.rawQuery ? query : query.toString(),
    stream: getStream(query),
    matrix: matrix,
    duration: duration
  }
}

/**
 * @param name {string}
 * @param root {Token}
 * @returns {string}
 */
const getLabelValue = (name, root) => {
  const tag = root.Children('log_stream_selector_rule').find(t => t.Child('label').value === name)
  if (!tag) {
    return null
  }
  if (tag.Child('operator').value !== '=') {
    throw new Error('Unsupported operator ' + tag.Child('operator').value)
  }
  return JSON.parse(tag.Child('quoted_str').value)
}

class Labels extends Sql.Raw {
  constructor () {
    super()
    this.labels = []
  }

  addLbl (name, sql) {
    this.labels.push([name, sql])
  }

  getLbl (name) {
    return (this.labels.find(l => l[0] === name) || [undefined, undefined])[1]
  }

  toString () {
    return '[' + this.labels.filter(l => l[0].substr(0, 2) !== '--')
      .map(l => `(${Sql.val(l[0])}, ${l[1]})`).join(',') + ']::Array(Tuple(String, String))'
  }
}

const labelFilterOps = {
  '=': (lbl, val) => {
    return `position(${lbl}, ${val}) != 0`
  },
  '!=': (lbl, val) => {
    return `position(${lbl}, ${val}) == 0`
  },
  '=~': (lbl, val) => {
    return `match(${lbl}, ${val}) == 1`
  },
  '!~': (lbl, val) => {
    return `match(${lbl}, ${val}) == 0`
  },
  '==': (lbl, val) => {
    return `${lbl} == ${val}`
  },
  '!==': (lbl, val) => {
    return `${lbl} != ${val}`
  },
  '>': (lbl, val) => {
    return `${lbl} > ${val}`
  },
  '>=': (lbl, val) => {
    return `${lbl} >= ${val}`
  },
  '<': (lbl, val) => {
    return `${lbl} < ${val}`
  },
  '<=': (lbl, val) => {
    return `${lbl} <= ${val}`
  }
}
