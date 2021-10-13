const {getDuration, concat_labels, apply_via_stream} = require("../common");
const {parseLabels} = require("../../../common");
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
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param value_expr {string}
 * @param last_value {boolean} if the applier should take the latest value in step (if step > duration)
 * @returns {registry_types.Request}
 */
function apply_via_request(token, query, value_expr, last_value) {
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
    const argMin = last_value ? 'argMin' : 'argMax';
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
                `${argMin}(value,timestamp_ms) as value`
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

module.exports = {
    apply_via_stream: apply_via_stream,
    rate: builder((token, query) => {
        const duration = getDuration(token, query);
        return apply_via_request(token, query, `SUM(unwrapped) / ${duration / 1000}`)
    }, (token, query) => {
        const duration = getDuration(token, query);
        return apply_via_stream(token, query,
            (sum, val) => sum+val.unwrapped,
            (sum) => sum / duration * 1000);
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
        return apply_via_stream(token, query,
            (sum, val) => sum + val.unwrapped,
            (sum) => sum);
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
            return sum ? {count: sum.count + 1, val: sum.val + val.unwrapped} : {count: 1, val: val.unwrapped}
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
            return Math.max(sum, val.unwrapped)
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
            return Math.min(sum, val.unwrapped)
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
        return apply_via_stream(token, query, (sum, val, time ) => {
            return sum && sum.time < time ? sum : {time: time, first: val.unwrapped}
        }, (sum) => sum.first);
    }),
    /**
     * last_over_time(unwrapped-range): the last value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    last_over_time: builder((token, query) => {
        return apply_via_request(token, query, 'argMax(unwrapped, uw_rate_a.timestamp_ms)', true);
    }, (token, query) => {
        return apply_via_stream(token, query, (sum, val, time) => {
            return sum && sum.time > time ? sum : {time: time, first: val.unwrapped}
        }, (sum) => sum.first);
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