const { _and } = require('../common')
/**
 *
 * @param token {Token}
 * @returns {number}
 */
function getVal (token) {
  const valTok = token.Child('compared_agg_statement_cmp').Child('number_value')
  if (valTok.Child('duration_value') || valTok.Child('bytes_value')) {
    throw new Error('Not Implemented')
  }
  return parseFloat(valTok.value.toString())
}

/**
 *
 * @param query {registry_types.Request}
 * @param streamProc {(function({value: number}): boolean)}
 * @param whereClause {string}
 * @returns {registry_types.Request}
 */
function genericReq (query, streamProc, whereClause) {
  if (query.stream && query.stream.length) {
    return {
      ...query,
      stream: [
        ...(query.stream || []),
        (s) => s.filter((e) => e.EOF || streamProc(e))
      ]
    }
  }
  if (query.group_by && query.group_by.length) {
    return {
      ...query,
      having: _and(query.having || [], [whereClause])
    }
  }
  return _and(query, [whereClause])
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq = (token, query) => {
  const val = getVal(token)
  return genericReq(query,
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
  const val = getVal(token)
  return genericReq(query,
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
  const val = getVal(token)
  return genericReq(query,
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
  const val = getVal(token)
  return genericReq(query,
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
  const val = getVal(token)
  return genericReq(query,
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
  const val = getVal(token)
  return genericReq(query,
    (e) => e.value <= val,
        `value <= ${val}`
  )
}
