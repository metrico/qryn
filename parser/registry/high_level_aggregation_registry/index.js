const _i = () => { throw new Error('Not implemented') }
const reg = require('./high_level_agg_reg')
const { genericRequest } = reg

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  sum: genericRequest('sum(value)', reg.streamSum),
  min: genericRequest('min(value)', reg.streamMin),
  max: genericRequest('max(value)', reg.streamMax),
  avg: genericRequest('avg(value)', reg.streamAvg),
  stddev: genericRequest('stddevPop(value)', reg.streamStddev),
  stdvar: genericRequest('varPop(value)', reg.streamStdvar),
  count: genericRequest('count(1)', reg.streamCount),
  bottomk: _i,
  topk: _i
}
