const _i = () => { throw new Error('Not implemented'); };
const reg = require("./high_level_agg_reg");
const {generic_request} = reg;


module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    sum: generic_request('sum(value)', reg.stream_sum),
    min: generic_request('min(value)', reg.stream_min),
    max: generic_request('max(value)', reg.stream_max),
    avg: generic_request('avg(value)', reg.stream_avg),
    stddev: generic_request('stddevPop(value)', reg.stream_stddev),
    stdvar: generic_request('varPop(value)', reg.stream_stdvar),
    count: generic_request('count(1)', reg.stream_count),
    bottomk: _i,
    topk: _i
};