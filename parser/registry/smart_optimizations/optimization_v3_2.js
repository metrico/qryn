const { getDuration } = require('../common')
const reg = require('./log_range_agg_reg_v3_2')
const Sql = require('@cloki/clickhouse-sql')
const { DATABASE_NAME, checkVersion } = require('../../../lib/utils')
const streamSelectorReg = require('../stream_selector_operator_registry')
const aggOpReg = require('../high_level_aggregation_registry')

/**
 *
 * @param token {Token}
 * @param fromMS {number}
 * @returns {boolean}
 */
module.exports.isApplicable = (token, fromMS) => {
  let logAggFn = token.Child('log_range_aggregation_fn')
  logAggFn = logAggFn ? logAggFn.value : null
  if (!logAggFn) {
    return false
  }
  const durationMs = getDuration(token)
  return checkVersion('v3_2', fromMS) &&
    !token.Child('log_pipeline') && reg[logAggFn] && durationMs >= 15000 && durationMs % 15000 === 0
}

/**
 *
 * @param token {Token}
 * @param fromNS {number}
 * @param toNS {number}
 * @param stepNS {number}
 */
module.exports.apply = (token, fromNS, toNS, stepNS) => {
  fromNS = Math.floor(fromNS / 15000000000) * 15000000000
  const tsClause = toNS
    ? Sql.between('samples.timestamp_ns', fromNS, toNS)
    : Sql.Gt('samples.timestamp_ns', fromNS)
  let q = (new Sql.Select())
    .select(['samples.fingerprint', 'fingerprint'])
    .from([`${DATABASE_NAME()}.metrics_15s`, 'samples'])
    .where(tsClause)
  q.join(`${DATABASE_NAME()}.time_series`, 'left any',
    Sql.Eq('samples.fingerprint', new Sql.Raw('time_series.fingerprint')))
  q.select([new Sql.Raw('any(JSONExtractKeysAndValues(time_series.labels, \'String\'))'), 'labels'])

  q.ctx = {
    step: stepNS / 1000000000
  }

  for (const streamSelectorRule of token.Children('log_stream_selector_rule')) {
    q = streamSelectorReg[streamSelectorRule.Child('operator').value](streamSelectorRule, q)
  }

  const lra = token.Child('log_range_aggregation')
  q = reg[lra.Child('log_range_aggregation_fn').value](lra, q)

  const aggOp = token.Child('aggregation_operator')
  if (aggOp) {
    q = aggOpReg[aggOp.Child('aggregation_operator_fn').value](aggOp, q)
  }

  return q
}
