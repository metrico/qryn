const reg = require('./stream_selector_operator_registry/stream_selector_operator_registry')
const numreg = require('./number_operator_registry/compared_label_reg')
const { hasExtraLabels, hasStream, addStream } = require('./common')
const Sql = require('@cloki/clickhouse-sql')

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports = (token, query) => {
  if (hasStream(query)) {
    const pred = processStreamExpression(token, query)
    return addStream(query,
      /**
       *
       * @param e {DataStream}
       * @returns {DataStream}
       */
      (e) => e.filter(pred))
  }
  const ex = processWhereExpression(token, query)
  return hasExtraLabels(query)
    ? query.where(ex)
    : reg.simpleAnd(query, ex)
}

/**
 *
 * @param andOr {string}
 * @param cond {Conditions}
 */
const checkAndOrType = (andOr, cond) => {
  return (andOr === 'and' && cond instanceof Sql.Conjunction) ||
    (andOr === 'or' && cond instanceof Sql.Disjunction)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Conditions}
 */
const processWhereExpression = (token, query) => {
  let where = null
  let andOr = null
  for (const t of token.tokens) {
    if (t.name === 'and_or') {
      andOr = t.value
      continue
    }
    andOr = (andOr || 'and').toLowerCase()
    let ex = null
    if (t.name === 'label_filter_expression') {
      ex = getLabelFilterWhereExpression(t, query)
    } else if (t.name === 'bracketed_label_filter_expression' || t.name === 'complex_label_filter_expression') {
      ex = processWhereExpression(t, query)
    } else {
      continue
    }
    if (!where) {
      where = ex
    } else if (checkAndOrType(andOr, where)) {
      where.args.push(ex)
    } else if (andOr === 'and') {
      where = Sql.And(where, ex)
    } else if (andOr === 'or') {
      where = Sql.Or(where, ex)
    }
    andOr = null
  }
  return where
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {string | string[]}
 */
const getLabelFilterWhereExpression = (token, query) => {
  // TODO:
  let clauses = null
  if (token.Child('string_label_filter_expression')) {
    switch (token.Child('operator').value) {
      case '=':
        clauses = hasExtraLabels(query) ? reg.eqExtraLabels(token) : reg.eqSimple(token)
        break
      case '!=':
        clauses = hasExtraLabels(query) ? reg.neqExtraLabels(token) : reg.neqSimple(token)
        break
      case '=~':
        clauses = hasExtraLabels(query) ? reg.regExtraLabels(token) : reg.regSimple(token)
        break
      case '!~':
        clauses = hasExtraLabels(query) ? reg.nregExtraLabels(token) : reg.nregSimple(token)
        break
      default:
        throw new Error('Unsupported operator')
    }
    return clauses
  }
  if (token.Child('number_label_filter_expression')) {
    const label = token.Child('label').value
    if (token.Child('duration_value') || token.Child('bytes_value')) {
      throw new Error('Not supported')
    }
    const val = token.Child('number_value').value
    const idx = hasExtraLabels(query) ? 'extraLabelsWhere' : 'simpleWhere'
    switch (token.Child('number_operator').value) {
      case '==':
        return numreg[idx].eq(label, val)
      case '!=':
        return numreg[idx].neq(label, val)
      case '>':
        return numreg[idx].gt(label, val)
      case '>=':
        return numreg[idx].ge(label, val)
      case '<':
        return numreg[idx].lt(label, val)
      case '<=':
        return numreg[idx].le(label, val)
    }
  }
}

/**
 *
 * @param fns {(function({labels: Object}): boolean)}
 * @returns {function({labels: Object}): boolean}
 */
const genericAnd = (...fns) => {
  return (e) => !fns.some(fn => !fn(e))
}

/**
 *
 * @param fns {(function({labels: Object}): boolean)}
 * @returns {function({labels: Object}): boolean}
 */
const genericOr = (...fns) => {
  return (e) => fns.some(fn => fn(e))
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
const processStreamExpression = (token, query) => {
  let andOr = 'and'
  let res = null
  for (const t of token.tokens) {
    if (t.name === 'label_filter_expression') {
      if (!res) {
        res = getLabelFilterStreamExpression(t, query)
        continue
      }
      res = (andOr || 'and').toLowerCase() === 'and'
        ? genericAnd(res, getLabelFilterStreamExpression(t, query))
        : genericOr(res, getLabelFilterStreamExpression(t, query))
    }
    if (t.name === 'bracketed_label_filter_expression' || t.name === 'complex_label_filter_expression') {
      if (!res) {
        res = processStreamExpression(t, query)
        continue
      }
      res = (andOr || 'and').toLowerCase() === 'and'
        ? genericAnd(res, processStreamExpression(t, query))
        : genericOr(res, processStreamExpression(t, query))
    }
    if (t.name === 'and_or') {
      andOr = t.value
    }
  }
  return res || (() => true)
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
const getLabelFilterStreamExpression = (token, query) => {
  if (token.Child('string_label_filter_expression')) {
    switch (token.Child('operator').value) {
      case '=':
        return reg.eqStream(token, query)
      case '!=':
        return reg.neqStream(token, query)
      case '=~':
        return reg.regStream(token, query)
      case '!~':
        return reg.nregStream(token, query)
      default:
        throw new Error('Unsupported operator')
    }
  }
  if (token.Child('number_label_filter_expression')) {
    const label = token.Child('label').value
    if (token.Child('duration_value') || token.Child('bytes_value')) {
      throw new Error('Not supported')
    }
    const val = token.Child('number_value').value
    switch (token.Child('number_operator').value) {
      case '==':
        return numreg.streamWhere.eq(label, val)
      case '!=':
        return numreg.streamWhere.neq(label, val)
      case '>':
        return numreg.streamWhere.gt(label, val)
      case '>=':
        return numreg.streamWhere.ge(label, val)
      case '<':
        return numreg.streamWhere.lt(label, val)
      case '<=':
        return numreg.streamWhere.le(label, val)
    }
  }
}
