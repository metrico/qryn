const {durationToMs, getDuration} = require("../common");
const {parseLabels, hashLabels} = require("../../../common");
/**
 *
 * @param via_request {function(Token, registry_types.Request): registry_types.Request}
 * @param via_stream {function(Token, registry_types.Request): registry_types.Request}
 * @returns { {
 *  via_request: function(Token, registry_types.Request): registry_types.Request,
 *  via_stream: function(Token, registry_types.Request): registry_types.Request} }
 */
function builder(via_request, via_stream) {
    return {
        via_request: via_request,
        via_stream: via_stream
    };
}

/**
 *
 * @param query {registry_types.Request}
 * @returns {string}
 */
function concat_labels(query) {
    if (query.select.some(f => f.endsWith('as extra_labels'))) {
        return `arraySort(arrayConcat(arrayFilter(`+
            `x -> arrayExists(y -> y.1 == x.1, extra_labels) == 0, `+
            `JSONExtractKeysAndValues(labels, 'String')), extra_labels))`;
    }
    return `JSONExtractKeysAndValues(labels, 'String')`
}

/**
 * sum_over_time(unwrapped-range): the sum of all values in the specified interval.
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {string}
 */
function apply_by_without_labels(token, query) {
    let labels = concat_labels(query);
    const filter_labels = token.Children('label').map(l => l.value).map(l => `'${l}'`);
    if (token.Child('by_without').value === 'by') {
        labels = `arraySort(arrayFilter(x -> arrayExists(y -> x.1 == y, [${filter_labels.join(',')}]) != 0, `+
            `${labels}))`;
    }
    if (token.Child('by_without').value === 'without') {
        labels = `arraySort(arrayFilter(x -> arrayExists(y -> x.1 == y, [${filter_labels.join(',')}]) == 0, `+
            `${labels}))`;
    }
    return labels;
}

/**
 * sum_over_time(unwrapped-range): the sum of all values in the specified interval.
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
function apply_by_without_stream(token, query) {
    const is_by = token.Child('by_without').value === 'by';
    const filter_labels = token.Children('label').map(l => l.value);
    return {
        ...query,
        stream: [...(query.stream ? query.stream : []),
            /**
             *
             * @param stream {DataStream}
             */
            (stream) => stream.map(e => {
                if (!e || !e.labels) {
                    return e;
                }
                let labels = [...Object.entries(e.labels)].filter(l =>
                    (is_by && filter_labels.includes(l[0])) || (!is_by && !filter_labels.includes(l[0]))
                );
                return {...e, labels: parseLabels(labels)};
            })
        ]
    };
}

/**
 *
 * @param values {Object}
 * @param timestamp {number}
 * @param value {number}
 * @param duration {number}
 * @param step {number}
 * @param counter_fn {function(any, any): any}
 * @returns {Object}
 */
function add_timestamp(values, timestamp, value, duration, step, counter_fn) {
    const timestamp_without_step = Math.floor(timestamp / duration) * duration
    const timestamp_with_step = step > duration ? Math.floor(timestamp_without_step / step) * step :
        timestamp_without_step;
    if (!values) {
        values = {};
    }
    if (!values[timestamp_with_step]) {
        values[timestamp_with_step] = {}
    }
    if (!values[timestamp_with_step][timestamp_without_step]) {
        values[timestamp_with_step][timestamp_without_step] = 0;
    }
    values[timestamp_with_step][timestamp_without_step] =
        counter_fn(values[timestamp_with_step][timestamp_without_step], value);
    return values;
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param value_expr {string}
 * @returns {registry_types.Request}
 */
function apply_via_request(token, query, value_expr) {
    let labels = "";
    if (token.Child('by_without')) {
        labels = apply_by_without_labels(token.Child('opt_by_without'), query);
    } else {
        labels = concat_labels(query);
    }
    const duration = getDuration(token, query);
    const step = query.ctx.step;
    /**
     *
     * @type {registry_types.Request}
     */
    const grouping_query = {
        select: [
            `${labels} as labels`,
            `floor(timestamp_ms / ${duration}) * ${duration} as timestamp_ms`,
            `${value_expr} as value`
        ],
        from: 'uw_rate_a',
        group_by: ['labels', `timestamp_ms`],
        order_by: {
            name: ["labels", "timestamp_ms"],
            order: "asc"
        }
    }
    /**
     *
     * @type {registry_types.Request}
     */
    return {
        stream: query.stream,
        ctx: {...query.ctx, duration: duration},
        matrix: true,
        with: {
            ...query.with,
            uw_rate_a: {
                ...query,
                stream: undefined,
                with: undefined,
                ctx: undefined,
                matrix: undefined,
                limit: undefined
            },
            uw_rate_b: step > duration ? grouping_query : undefined
        },
        ...(step > duration ? {
            select: [
                `labels`, `floor(uw_rate_b.timestamp_ms / ${step}) * ${step} as timestamp_ms`,
                'sum(value) as value'
            ],
            from: 'uw_rate_b',
            group_by: ['labels', 'timestamp_ms'],
            order_by: {
                name: ['labels', 'timestamp_ms'],
                order: 'asc'
            }
        } : grouping_query)
    }
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param counter_fn {function(any, any): any}
 * @param summarize_fn {function(any): number}
 * @returns {registry_types.Request}
 */
function apply_via_stream(token, query, counter_fn, summarize_fn) {
    if (token.Child('by_without')) {
        query = apply_by_without_stream(token.Child('opt_by_without'), query);
    }
    let results = new Map();
    const duration = getDuration(token, query);
    const step = query.ctx.step;
    return {
        ...query,
        ctx: {...query.ctx, duration: duration},
        matrix: true,
        stream: [...(query.stream ? query.stream : []),
            /**
             * @param s {DataStream}
             */
                (s) => s.remap((emit, e) => {
                if (!e || !e.labels) {
                    for (const [_, v] of results) {
                        const ts = [...Object.entries(v.values)];
                        ts.sort();
                        for (const _v of ts) {
                            // v  / (duration / 1000)
                            const value = Object.values(_v[1]).reduce((sum, v) => sum + summarize_fn(v), 0);
                            emit({labels: v.labels, timestamp_ms: _v[0], value: value});
                        }
                    }
                    results = new Map();
                    emit({EOF: true})
                    return;
                }
                const l = hashLabels(e.labels);
                if (!results.has(l)) {
                    results.set(l, {
                        labels: e.labels,
                        values: add_timestamp(undefined, e.timestamp_ms, e.unwrapped, duration, step, counter_fn)
                    });
                } else {
                    results.get(l).values = add_timestamp(
                        results.get(l).values, e.timestamp_ms, e.unwrapped, duration, step, counter_fn
                    );
                }
            })
        ]
    };
}

module.exports = {
    rate: builder((token, query) => {
        const duration = getDuration(token, query);
        return apply_via_request(token, query, `SUM(unwrapped) / ${duration / 1000}`)
    }, (token, query) => {
        const duration = getDuration(token, query);
        return apply_via_stream(token, query, (sum, val) => sum+val, (sum) => sum / duration * 1000);
    }),

    /**
     * sum_over_time(unwrapped-range): the sum of all values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    sum_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'sum(unwrapped)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => sum + val, (sum) => sum);
    }),

    /**
     * avg_over_time(unwrapped-range): the average value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    avg_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'avg(unwrapped)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => {
            return sum ? {count: sum.count + 1, val: sum.val + val} : {count: 1, val: val}
        }, (sum) => sum.val / sum.count);
    }),
    /**
     * max_over_time(unwrapped-range): the maximum value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    max_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'max(unwrapped)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => {
            return Math.max(sum, val)
        }, (sum) => sum);
    }),
    /**
     * min_over_time(unwrapped-range): the minimum value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    min_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'min(unwrapped)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => {
            return Math.min(sum, val)
        }, (sum) => sum);
    }),
    /**
     * first_over_time(unwrapped-range): the first value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    first_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'argMin(unwrapped, uw_rate_a.timestamp_ms)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => {
            return sum ? sum : {first: val}
        }, (sum) => sum.first);
    }),
    /**
     * last_over_time(unwrapped-range): the last value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    last_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'argMax(unwrapped, uw_rate_a.timestamp_ms)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => {
            return val
        }, (sum) => sum);
    }),
    /**
     * stdvar_over_time(unwrapped-range): the population standard variance of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    stdvar_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'varPop(unwrapped)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => {
            throw new Error('not implemented')
        }, (sum) => sum);
    }),
    /**
     * stddev_over_time(unwrapped-range): the population standard deviation of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    stddev_over_time:  builder((token, query) => {
        return apply_via_request(token, query, 'stddevPop(unwrapped)');
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val) => {
            throw new Error('not implemented')
        }, (sum) => sum);
    }),
    /**
     * quantile_over_time(scalar,unwrapped-range): the φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    quantile_over_time: (token, query) => {
    },
    /**
     * absent_over_time(unwrapped-range): returns an empty vector if the range vector passed to it has any elements and a 1-element vector with the value 1 if the range vector passed to it has no elements. (absent_over_time is useful for alerting on when no time series and logs stream exist for label combination for a certain amount of time.)
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    absent_over_time: (token, query) => {
    }
}