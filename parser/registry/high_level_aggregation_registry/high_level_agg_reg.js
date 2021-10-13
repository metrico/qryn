const {apply_via_stream} = require("../common");

/**
 *
 * @param token {Token}
 * @returns [string, string[]]
 */
function get_by_without(token) {
    return token.Child('by_without') ?
        [
            token.Child('by_without').value.toString().toLowerCase(),
            token.Child('opt_by_without').Children('label').map(c => c.value)
        ] : [ undefined, undefined ];
}

/**
 *
 * @param expression {string}
 * @param stream {(function(Token, registry_types.Request): registry_types.Request)}
 * @returns {(function(Token, registry_types.Request): registry_types.Request)}
 */
module.exports.generic_request = (expression, stream) => {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    return (token, query) => {
        if (query.stream && query.stream.length) {
            return stream(token, query);
        }
        const [by_without, label_list] = get_by_without(token);
        if (!by_without) {
            return query;
        }
        const labels_filter_clause = `arrayFilter(x -> x.1 ${by_without === 'by' ? 'IN' : 'NOT IN'} `+
            `(${label_list.map(l => `'${l}'`).join(',')}), labels)`;
        return {
            ctx: query.ctx,
            with: {
                ...(query.with ? query.with : {}),
                agg_a: {
                    ...query,
                    ctx: undefined,
                    with: undefined,
                    stream: undefined
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
                name: ['labels', 'timestamp_ms'],
                order: 'asc'
            },
            matrix: true,
            stream: query.stream
        }
    };
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.stream_sum = (token, query) => {
    return apply_via_stream(token, query, (sum, e) => {
        sum = sum || 0;
        return sum + e.value;
    }, (sum) => sum, false);
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.stream_min = (token, query) => {
    return apply_via_stream(token, query, (sum, e) => {
        return sum ? Math.min(sum.value, e.value) : { value: e.value };
    }, sum => sum.value);
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.stream_max = (token, query) => {
    return apply_via_stream(token, query, (sum, e) => {
        return sum ? Math.max(sum.value, e.value) : { value: e.value };
    }, sum => sum.value);
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.stream_avg = (token, query) => {
    return apply_via_stream(token, query, (sum, e) => {
        return sum ? { value: sum.value + e.value, count: sum.count + 1 } : { value: e.value, count: 1 };
    }, sum => sum.value / sum.count);
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.stream_stddev = (token, query) => {
    throw new Error("Not implemented");
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.stream_stdvar = (token, query) => {
    throw new Error("Not implemented");
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.stream_count = (token, query) => {
    return apply_via_stream(token, query, (sum) => {
        return sum ? sum + 1 : 1;
    }, sum => sum);
}