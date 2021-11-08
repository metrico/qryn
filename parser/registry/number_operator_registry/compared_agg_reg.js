const { _and } = require('../common')
/**
 *
 * @param token {Token}
 * @returns {number}
 */
function get_val (token) {
  const val_tok = token.Child('compared_agg_statement_cmp').Child('number_value')
  if (val_tok.Child('duration_value') || val_tok.Child('bytes_value')) {
    throw new Error('Not Implemented')
  }
  return parseFloat(val_tok.value.toString())
}

/**
 *
 * @param query {registry_types.Request}
 * @param stream_proc {(function({value: number}): boolean)}
 * @param where_clause {string}
 * @returns {registry_types.Request}
 */
function generic_req (query, stream_proc, where_clause) {
  if (query.stream && query.stream.length) {
    return {
      ...query,
      stream: [
        ...(query.stream || []),
        (s) => s.filter((e) => e.EOF || stream_proc(e))
      ]
    }
  }
  if (query.group_by && query.group_by.length) {
    return {
      ...query,
      having: _and(query.having || [], [where_clause])
    }
  }
  return _and(query, [where_clause])
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq = (token, query) => {
  const val = get_val(token)
  return generic_req(query,
    (e) => Math.abs(e.value - val) < 0.0000000001,
        `value == ${val}`
  )
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.neq = (token, query) => {
  const val = get_val(token)
  return generic_req(query,
    (e) => Math.abs(e.value - val) > 0.0000000001,
        `value != ${val}`
  )
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.gt = (token, query) => {
  const val = get_val(token)
  return generic_req(query,
    (e) =>
      e.value > val,
        `value > ${val}`
  )
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.ge = (token, query) => {
  const val = get_val(token)
  return generic_req(query,
    (e) => e.value >= val,
        `value >= ${val}`
  )
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.lt = (token, query) => {
  const val = get_val(token)
  return generic_req(query,
    (e) => e.value < val,
        `value < ${val}`
  )
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.le = (token, query) => {
  const val = get_val(token)
  return generic_req(query,
    (e) => e.value <= val,
        `value <= ${val}`
  )
}
