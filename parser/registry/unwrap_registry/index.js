const reg = require('./unwrap_registry');

module.exports = {
    /**
     * rate(unwrapped-range): calculates per second rate of all values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    rate: (token, query) => {
        if (query.stream) {
            return reg.rate.via_stream(token, query);
        }
        return reg.rate.via_request(token, query);
    },
    /**
     * sum_over_time(unwrapped-range): the sum of all values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    sum_over_time: (token, query) => {
        if (query.stream) {
            return reg.sum_over_time.via_stream(token, query);
        }
        return reg.sum_over_time.via_request(token, query);
    },
    /**
     * avg_over_time(unwrapped-range): the average value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    avg_over_time: (token, query) => {
        if (query.stream) {
            return reg.avg_over_time.via_stream(token, query);
        }
        return reg.avg_over_time.via_request(token, query);
    },
    /**
     * max_over_time(unwrapped-range): the maximum value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    max_over_time: (token, query) => {
        if (query.stream) {
            return reg.max_over_time.via_stream(token, query);
        }
        return reg.max_over_time.via_request(token, query);
    },
    /**
     * min_over_time(unwrapped-range): the minimum value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    min_over_time: (token, query) => {
        if (query.stream) {
            return reg.min_over_time.via_stream(token, query);
        }
        return reg.min_over_time.via_request(token, query);
    },
    /**
     * first_over_time(unwrapped-range): the first value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    first_over_time: (token, query) => {
        if (query.stream) {
            return reg.first_over_time.via_stream(token, query);
        }
        return reg.first_over_time.via_request(token, query);
    },
    /**
     * last_over_time(unwrapped-range): the last value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    last_over_time: (token, query) => {
        if (query.stream) {
            return reg.last_over_time.via_stream(token, query);
        }
        return reg.last_over_time.via_request(token, query);
    },
    /**
     * stdvar_over_time(unwrapped-range): the population standard variance of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    stdvar_over_time: (token, query) => {
        if (query.stream) {
            return reg.stdvar_over_time.via_stream(token, query);
        }
        return reg.stdvar_over_time.via_request(token, query);
    },
    /**
     * stddev_over_time(unwrapped-range): the population standard deviation of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    stddev_over_time: (token, query) => {
        if (query.stream) {
            return reg.stddev_over_time.via_stream(token, query);
        }
        return reg.stddev_over_time.via_request(token, query);
    },
    /**
     * quantile_over_time(scalar,unwrapped-range): the φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    quantile_over_time: (token, query) => {
        if (query.stream) {
            return reg.quantile_over_time.via_stream(token, query);
        }
        return reg.quantile_over_time.via_request(token, query);
    },
    /**
     * absent_over_time(unwrapped-range): returns an empty vector if the range vector passed to it has any elements and a 1-element vector with the value 1 if the range vector passed to it has no elements. (absent_over_time is useful for alerting on when no time series and logs stream exist for label combination for a certain amount of time.)
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    absent_over_time: (token, query) => {
        if (query.stream) {
            return reg.absent_over_time.via_stream(token, query);
        }
        return reg.absent_over_time.via_request(token, query);
    }
}