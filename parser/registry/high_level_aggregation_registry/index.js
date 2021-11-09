const _i = () => { throw new Error('Not implemented') }
const reg = require('./high_level_agg_reg')
const { genericRequest } = reg

module.exports = {
  /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  sum: genericRequest('sum(value)', reg.stream_sum),
  min: genericRequest('min(value)', reg.stream_min),
  max: genericRequest('max(value)', reg.stream_max),
  avg: genericRequest('avg(value)', reg.stream_avg),
  stddev: genericRequest('stddevPop(value)', reg.stream_stddev),
  stdvar: genericRequest('varPop(value)', reg.stream_stdvar),
  count: genericRequest('count(1)', reg.stream_count),
  bottomk: _i,
  topk: _i
}
