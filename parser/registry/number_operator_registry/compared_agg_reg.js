const { hasStream, addStream } = require('../common')
const Sql = require('@cloki/clickhouse-sql')
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
 * @param query {Select}
 * @param streamProc {(function({value: number}): boolean)}
 * @param whereClause {Conditions | Condition}
 * @returns {Select}
 */
function genericReq (query, streamProc, whereClause) {
  if (hasStream(query)) {
    return addStream(query, (s) => s.filter((e) => e.EOF || streamProc(e)))
  }
  if (query.aggregations.length) {
    return query.having(whereClause)
  }
  return query.where(whereClause)
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.eq = (token, query) => {
  const val = getVal(token)
  return genericReq(query,
    (e) => Math.abs(e.value - val) < 0.0000000001,
    Sql.Eq('value', val)
  )
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.neq = (token, query) => {
  const val = getVal(token)
  return genericReq(query,
    (e) => Math.abs(e.value - val) > 0.0000000001,
    Sql.Ne('value', val)
  )
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.gt = (token, query) => {
  const val = getVal(token)
  return genericReq(query,
    (e) =>
      e.value > val,
    Sql.Gt('value', val)
  )
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.ge = (token, query) => {
  const val = getVal(token)
  return genericReq(query,
    (e) => e.value >= val,
    Sql.Gte('value', val)
  )
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.lt = (token, query) => {
  const val = getVal(token)
  return genericReq(query,
    (e) => e.value < val,
    Sql.Lt('value', val)
  )
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.le = (token, query) => {
  const val = getVal(token)
  return genericReq(query,
    (e) => e.value <= val,
    Sql.Lte('value', val)
  )
}
