const {durationToMs} = require("./common");

/**
 *
 * @param value_expr {string}
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
const generic_rate = (value_expr, token, query) => {
    const duration = durationToMs(token.Child('duration_value').value);
    const step = query.ctx.step;
    /**
     *
     * @type {registry_types.Request}
     */
    /*const query_gaps = {
        select: [
            'a1.labels',
            `toFloat64(${Math.floor(query.ctx.start / duration) * duration} + number * ${duration}) as timestamp_ms`,
            'toFloat64(0) as value'
        ],
        from: `(SELECT DISTINCT labels FROM rate_a) as a1, numbers(${Math.floor((query.ctx.end - query.ctx.start) / duration)}) as a2`,
    };*/
    return {
        ctx: { ...query.ctx, duration: duration },
        with: {
            rate_a: {
                ...query,
                ctx: undefined,
                with: undefined,
                limit: undefined
            },
            rate_b: {
                select: [
                    'labels',
                    `floor(timestamp_ms / ${duration}) * ${duration} as timestamp_ms`,
                    `${value_expr} as value`
                ],
                from: 'rate_a',
                group_by: ['labels', `timestamp_ms`],
                order_by: {
                    name: ["labels", "timestamp_ms"],
                    order: "asc"
                }
            },
            rate_c: step > duration ? {
                select: [
                    'labels',
                    `floor(timestamp_ms / ${step}) * ${step} as timestamp_ms`,
                    `avg(rate_b.value) as value`
                ],
                from: 'rate_b',
                group_by: ['labels', `timestamp_ms`],
                order_by: {
                    name: ["labels", "timestamp_ms"],
                    order: "asc"
                }
            } : undefined
        },
        select: ['labels', 'timestamp_ms', 'sum(value) as value'],
        from: step > duration ? 'rate_c' : 'rate_b',
        group_by: ['labels', 'timestamp_ms'],
        order_by: {
            name: ['labels', 'timestamp_ms'],
            order: 'asc'
        },
        matrix: true
    };
}

module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    rate: (token, query) => {
        const duration = durationToMs(token.Child('duration_value').value);
        return generic_rate(`toFloat64(count(1)) * 1000 / ${duration}`, token, query);

    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    count_over_time: (token, query) => {
        return generic_rate(`toFloat64(count(1))`, token, query);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    bytes_rate: (token, query) => {
        const duration = durationToMs(token.Child('duration_value').value);
        return generic_rate(`toFloat64(sum(length(string))) * 1000 / ${duration}`, token, query);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    bytes_over_time: (token, query) => {
        return generic_rate(`toFloat64(sum(length(string)))`, token, query);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    absent_over_time: (token, query) => {
        const duration = durationToMs(token.Child('duration_value').value);
        const query_data = {...query};
        query_data.select = ['labels', `floor(timestamp_ms / ${duration}) * ${duration} as timestamp_ms`,
            `toFloat64(0) as value`];
        query_data.limit = undefined;
        query_data.group_by = ['labels', `floor(timestamp_ms / ${duration}) * ${duration}`];
        query_data.order_by = {
            name: "timestamp_ms",
            order: "asc"
        }
        query_data.matrix = true;
        /**
         *
         * @type {registry_types.Request}
         */
        const query_gaps = {
            select: [
                'a1.labels',
                `toFloat64(${Math.floor(query.ctx.start / duration) * duration} + number * ${duration}) as timestamp_ms`,
                'toFloat64(1) as value' //other than the generic
            ],
            from: `(SELECT DISTINCT labels FROM rate_a) as a1, numbers(${Math.floor((query.ctx.end - query.ctx.start) / duration)}) as a2`,
        };
        return {
            ctx: query.ctx,
            with: {
                rate_a: query_data,
                rate_b: query_gaps,
                rate_c: { requests: [{select: ['*'], from: 'rate_a'}, {select: ['*'], from: 'rate_b'}] }
            },
            select: ['labels', 'timestamp_ms', 'min(value) as value'], // other than the generic
            from: 'rate_c',
            group_by: ['labels', 'timestamp_ms'],
            order_by: {
                name: 'timestamp_ms',
                order: 'asc'
            },
            matrix: true
        };
    }
};