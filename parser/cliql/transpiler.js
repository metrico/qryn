const { DATABASE_NAME } = require('../../lib/utils')
const Sql = require('@cloki/clickhouse-sql')
const lineFilterOperatorRegistry = require('../registry/line_filter_operator_registry')
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

  const lbls = new Labels()
  let query = (new Sql.Select())
    .select(
      (msg ? [new Sql.Raw(msg), 'string'] : [new Sql.Raw(val), 'value']),
      [new Sql.Raw('1'), 'fingerprint'],
      (msg ? [new Sql.Raw(tsNS), 'timestamp_ns'] : [new Sql.Raw(`intDiv(${tsNS}, 1000000)`), 'timestamp_ns']),
      [lbls, 'labels']
    ).from([`${db}.${table}`, 'samples'])
    .where(Sql.And(Sql.Lt('samples.timestamp_ns', request.end), Sql.Gte('samples.timestamp_ns', request.start)))
    .limit(msg ? parseInt(request.limit) : 0)
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
    }
    if (pipeline.Child('parser_expression')) {
      const op = pipeline.Child('parser_fn_name').value
      query = parserRegistry[op](pipeline, query)
      continue
    }
    if (pipeline.Child('labels_format_expression')) {
      for (const param of pipeline.Children('labels_format_expression_param')) {
        if (param.Child('label_inject_param')) {
          lbls.addLbl(param.Child('label').value, JSON.parse(param.Child('quoted_str').value))
        }
        if (param.Child('label_rename_param')) {
          const _lbls = param.Children('label')
          lbls.addLbl(_lbls[0].value, _lbls[1].value)
        }
      }
    }
  }
  console.log(query.toString())
  return {
    query: request.rawQuery ? query : query.toString(),
    stream: getStream(query),
    matrix: !!val,
    duration: 1
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
    this.labels.push([Sql.val(name), new Sql.Raw(sql)])
  }

  toString () {
    return '[' + this.labels.map(l => `(${l[0]}, ${l[1]})`).join(',') + ']'
  }
}