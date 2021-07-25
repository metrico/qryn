const _i = () => { throw new Error('Not implemented'); };

/**
 *
 * @param expression {string}
 * @returns {(function(Token, registry_types.Request): registry_types.Request)}
 */
const generic_request = (expression) => {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    return (token, query) => {
        const by_without = token.Child('opt_by_without') ?
            token.Child('by_without').value.toString().toLowerCase() :
            undefined;
        const label_list = token.Child('opt_by_without') ?
            token.Child('opt_by_without').Children('label').map(c => c.value) : undefined;
        if (!by_without) {
            return query;
        }
        const labels_filter_clause = `arrayFilter(x -> x.1 ${by_without === 'by' ? 'IN' : 'NOT IN'} `+
            `(${label_list.map(l => `'${l}'`).join(',')}), `+
            `JSONExtractKeysAndValuesRaw(labels))`;
        return {
            ctx: query.ctx,
            with: {
                ...(query.with ? query.with : {}),
                agg_a: {
                    ...query,
                    ctx: undefined,
                    with: undefined
                }
            },
            select: [
                `${labels_filter_clause} as labels`,
                'timestamp_ms',
                `${expression} as value` //'sum(value) as value'
            ],
            from: 'agg_a',
            group_by: ['labels', 'timestamp_ms'],
            order_by: {
                name: 'timestamp_ms',
                order: 'asc'
            },
            matrix: true
        }
    };
}

module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    sum: generic_request('sum(value)'),
    min: generic_request('min(value)'),
    max: generic_request('max(value)'),
    avg: generic_request('avg(value)'),
    stddev: generic_request('stddevPop(value)'),
    stdvar: generic_request('varPop(value)'),
    count: generic_request('count(1)'),
    bottomk: _i,
    topk: _i
};