const reg = require('./stream_selector_operator_registry/stream_selector_operator_registry')
const numreg = require('./number_operator_registry/compared_label_reg')
const { hasExtraLabels, _and } = require('./common')

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports = (token, query) => {
  if (query.stream) {
    const pred = processStreamExpression(token, query)
    return {
      ...query,
      stream: [...(query.stream ? query.stream : []),
        /**
                 *
                 * @param e {DataStream}
                 * @returns {DataStream}
                 */
        (e) => e.filter(pred)
      ]
    }
  }
  let ex = processWhereExpression(token, query)
  ex = ex[0] === 'and' ? ex.slice(1) : [ex]
  return hasExtraLabels(query)
    ? _and(query, ex)
    : reg.simple_and(query, ex)
}

/**
 *
 * @param where {string[]}
 * @returns {string | string[]}
 */
const derefWhereExp = (where) => where.length > 0 && where.length < 3 ? where[1] : where

/**
 *
 * @param where1 {string | string[]}
 * @param op {string}
 * @param where2 {string | string[]}
 * @returns {string[]}
 */
const whereConcat = (where1, op, where2) => {
  where1 = Array.isArray(where1) ? derefWhereExp(where1) : where1
  let where = null
  if (Array.isArray(where1)) {
    where = [op, ...(where1[0] === op ? where1.slice(1) : [where1])]
  } else {
    where = [op, where1]
  }
  if (!where2) {
    return where
  }
  where2 = Array.isArray(where2) ? derefWhereExp(where2) : where2
  if (Array.isArray(where2)) {
    where.push.apply(where, where2[0] === op ? where2.slice(1) : [where2])
  } else {
    where.push(where2)
  }
  return where
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
const processWhereExpression = (token, query) => {
  let where = []
  let andOr = null
  for (const t of token.tokens) {
    if (t.name === 'label_filter_expression') {
      andOr = (andOr || 'and').toLowerCase()
      const ex = getLabelFilterWhereExpression(t, query)
      switch (where.length) {
        case 0:
        case 1:
          where = Array.isArray(ex) ? ex : [andOr, ex]
          break
        case 2:
          where = whereConcat(where[1], andOr, ex)
          break
        default:
          where = whereConcat(where, andOr, ex)
      }
      andOr = null
    }
    if (t.name === 'bracketed_label_filter_expression' || t.name === 'complex_label_filter_expression') {
      andOr = (andOr || 'and').toLowerCase()

      switch (where.length) {
        case 0:
        case 1:
          where = processWhereExpression(t, query)
          break
        case 2:
          where = whereConcat(where[1], andOr, processWhereExpression(t, query))
          break
        default:
          where = whereConcat(where, andOr, processWhereExpression(t, query))
      }
    }
    if (t.name === 'andOr') {
      andOr = t.value
    }
  }
  return where
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string | string[]}
 */
const getLabelFilterWhereExpression = (token, query) => {
  // TODO:
  let clauses = null
  if (token.Child('string_label_filter_expression')) {
    switch (token.Child('operator').value) {
      case '=':
        clauses = hasExtraLabels(query) ? reg.eq_extra_labels(token) : reg.eq_simple(token)
        break
      case '!=':
        clauses = hasExtraLabels(query) ? reg.neq_extra_labels(token) : reg.neq_simple(token)
        break
      case '=~':
        clauses = hasExtraLabels(query) ? reg.reg_extra_labels(token) : reg.nreg_extra_labels(token)
        break
      case '!~':
        clauses = hasExtraLabels(query) ? reg.nreg_extra_labels(token) : reg.nreg_simple(token)
        break
      default:
        throw new Error('Unsupported operator')
    }
    return derefWhereExp(['and', ...clauses])
  }
  if (token.Child('number_label_filter_expression')) {
    const label = token.Child('label').value
    if (token.Child('duration_value') || token.Child('bytes_value')) {
      throw new Error('Not supported')
    }
    const val = token.Child('number_value').value
    const idx = hasExtraLabels(query) ? 'extra_labels_where' : 'simple_where'
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
    if (t.name === 'andOr') {
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
        return reg.eq_stream(token, query)
      case '!=':
        return reg.neq_stream(token, query)
      case '=~':
        return reg.reg_stream(token, query)
      case '!~':
        return reg.nreg_stream(token, query)
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
        return numreg.stream_where.eq(label, val)
      case '!=':
        return numreg.stream_where.neq(label, val)
      case '>':
        return numreg.stream_where.gt(label, val)
      case '>=':
        return numreg.stream_where.ge(label, val)
      case '<':
        return numreg.stream_where.lt(label, val)
      case '<=':
        return numreg.stream_where.le(label, val)
    }
  }
}
