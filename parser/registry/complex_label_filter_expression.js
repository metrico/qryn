const reg = require('./stream_selector_operator_registry/stream_selector_operator_registry')
const numreg = require('./number_operator_registry/compared_label_reg')
const { has_extra_labels, _and } = require('./common')

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports = (token, query) => {
  if (query.stream) {
    const pred = process_stream_expression(token, query)
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
  let ex = process_where_expression(token, query)
  ex = ex[0] === 'and' ? ex.slice(1) : [ex]
  return has_extra_labels(query)
    ? _and(query, ex)
    : reg.simple_and(query, ex)
}

/**
 *
 * @param where {string[]}
 * @returns {string | string[]}
 */
const deref_where_exp = (where) => where.length > 0 && where.length < 3 ? where[1] : where

/**
 *
 * @param where_1 {string | string[]}
 * @param op {string}
 * @param where_2 {string | string[]}
 * @returns {string[]}
 */
const where_concat = (where_1, op, where_2) => {
  where_1 = Array.isArray(where_1) ? deref_where_exp(where_1) : where_1
  let where = null
  if (Array.isArray(where_1)) {
    where = [op, ...(where_1[0] === op ? where_1.slice(1) : [where_1])]
  } else {
    where = [op, where_1]
  }
  if (!where_2) {
    return where
  }
  where_2 = Array.isArray(where_2) ? deref_where_exp(where_2) : where_2
  if (Array.isArray(where_2)) {
    where.push.apply(where, where_2[0] === op ? where_2.slice(1) : [where_2])
  } else {
    where.push(where_2)
  }
  return where
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string[]}
 */
const process_where_expression = (token, query) => {
  let where = []
  let and_or = null
  for (const t of token.tokens) {
    if (t.name === 'label_filter_expression') {
      and_or = (and_or || 'and').toLowerCase()
      const ex = get_label_filter_where_expression(t, query)
      switch (where.length) {
        case 0:
        case 1:
          where = Array.isArray(ex) ? ex : [and_or, ex]
          break
        case 2:
          where = where_concat(where[1], and_or, ex)
          break
        default:
          where = where_concat(where, and_or, ex)
      }
      and_or = null
    }
    if (t.name === 'bracketed_label_filter_expression' || t.name === 'complex_label_filter_expression') {
      and_or = (and_or || 'and').toLowerCase()

      switch (where.length) {
        case 0:
        case 1:
          where = process_where_expression(t, query)
          break
        case 2:
          where = where_concat(where[1], and_or, process_where_expression(t, query))
          break
        default:
          where = where_concat(where, and_or, process_where_expression(t, query))
      }
    }
    if (t.name === 'and_or') {
      and_or = t.value
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
const get_label_filter_where_expression = (token, query) => {
  // TODO:
  let clauses = null
  if (token.Child('string_label_filter_expression')) {
    switch (token.Child('operator').value) {
      case '=':
        clauses = has_extra_labels(query) ? reg.eq_extra_labels(token) : reg.eq_simple(token)
        break
      case '!=':
        clauses = has_extra_labels(query) ? reg.neq_extra_labels(token) : reg.neq_simple(token)
        break
      case '=~':
        clauses = has_extra_labels(query) ? reg.reg_extra_labels(token) : reg.nreg_extra_labels(token)
        break
      case '!~':
        clauses = has_extra_labels(query) ? reg.nreg_extra_labels(token) : reg.nreg_simple(token)
        break
      default:
        throw new Error('Unsupported operator')
    }
    return deref_where_exp(['and', ...clauses])
  }
  if (token.Child('number_label_filter_expression')) {
    const label = token.Child('label').value
    if (token.Child('duration_value') || token.Child('bytes_value')) {
      throw new Error('Not supported')
    }
    const val = token.Child('number_value').value
    const idx = has_extra_labels(query) ? 'extra_labels_where' : 'simple_where'
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
const generic_and = (...fns) => {
  return (e) => !fns.some(fn => !fn(e))
}

/**
 *
 * @param fns {(function({labels: Object}): boolean)}
 * @returns {function({labels: Object}): boolean}
 */
const generic_or = (...fns) => {
  return (e) => fns.some(fn => fn(e))
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {function({labels: Object}): boolean}
 */
const process_stream_expression = (token, query) => {
  let and_or = 'and'
  let res = null
  for (const t of token.tokens) {
    if (t.name === 'label_filter_expression') {
      if (!res) {
        res = get_label_filter_stream_expression(t, query)
        continue
      }
      res = (and_or || 'and').toLowerCase() === 'and'
        ? generic_and(res, get_label_filter_stream_expression(t, query))
        : generic_or(res, get_label_filter_stream_expression(t, query))
    }
    if (t.name === 'bracketed_label_filter_expression' || t.name === 'complex_label_filter_expression') {
      if (!res) {
        res = process_stream_expression(t, query)
        continue
      }
      res = (and_or || 'and').toLowerCase() === 'and'
        ? generic_and(res, process_stream_expression(t, query))
        : generic_or(res, process_stream_expression(t, query))
    }
    if (t.name === 'and_or') {
      and_or = t.value
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
const get_label_filter_stream_expression = (token, query) => {
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
