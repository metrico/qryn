const { getDuration, preJoinLabels, dist, sharedParamNames } = require('../common')
const reg = require('./log_range_agg_reg_v3_2')
const Sql = require('@cloki/clickhouse-sql')
const { DATABASE_NAME, checkVersion } = require('../../../lib/utils')
const streamSelectorReg = require('../stream_selector_operator_registry')
const aggOpReg = require('../high_level_aggregation_registry')
const { clusterName } = require('../../../common')
const logger = require('../../../lib/logger')
const _dist = clusterName ? '_dist' : ''

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
    !isLogPipeline(token) && reg[logAggFn] && durationMs >= 15000 && durationMs % 15000 === 0
}

function isLogPipeline (token) {
  let isPipeline = false
  for (const pipeline of token.Children('log_pipeline')) {
    isPipeline |= !pipeline.Child('line_filter_operator') ||
      !(pipeline.Child('line_filter_operator').value === '|=' &&
        ['""', '``'].includes(pipeline.Child('quoted_str').value))
  }
  return isPipeline
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
  const fromParam = new Sql.Parameter(sharedParamNames.from)
  const toParam = new Sql.Parameter(sharedParamNames.to)
  const tsClause = toNS
    ? Sql.between('samples.timestamp_ns', fromNS, toNS)
    : Sql.Gt('samples.timestamp_ns', fromNS)
  let q = (new Sql.Select())
    .select(['samples.fingerprint', 'fingerprint'])
    .from([`${DATABASE_NAME()}.metrics_15s${_dist}`, 'samples'])
    .where(tsClause)
    .addParam(fromParam)
    .addParam(toParam)
  fromParam.set(fromNS)
  toParam.set(toNS)

  q.ctx = {
    step: stepNS / 1000000000,
    inline: !!clusterName
  }

  for (const streamSelectorRule of token.Children('log_stream_selector_rule')) {
    q = streamSelectorReg[streamSelectorRule.Child('operator').value](streamSelectorRule, q)
  }
  preJoinLabels(token, q, dist)
  q = q.groupBy('labels')

  const lra = token.Child('log_range_aggregation')
  q = reg[lra.Child('log_range_aggregation_fn').value](lra, q)

  const aggOp = token.Child('aggregation_operator')
  if (aggOp) {
    q = aggOpReg[aggOp.Child('aggregation_operator_fn').value](aggOp, q)
  }

  logger.debug(q.toString())

  return q
}
