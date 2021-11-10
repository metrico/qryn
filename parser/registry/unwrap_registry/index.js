const reg = require('./unwrap_registry')
const { getPlugins } = require('../common')

module.exports = {
  /**
     * rate(unwrapped-range): calculates per second rate of all values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  rate: (token, query) => {
    if (query.stream) {
      return reg.rate.viaStream(token, query)
    }
    return reg.rate.viaRequest(token, query)
  },
  /**
     * sumOverTime(unwrapped-range): the sum of all values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  sum_over_time: (token, query) => {
    if (query.stream) {
      return reg.sumOverTime.viaStream(token, query)
    }
    return reg.sumOverTime.viaRequest(token, query)
  },
  /**
     * avgOverTime(unwrapped-range): the average value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  avg_over_time: (token, query) => {
    if (query.stream) {
      return reg.avgOverTime.viaStream(token, query)
    }
    return reg.avgOverTime.viaRequest(token, query)
  },
  /**
     * maxOverTime(unwrapped-range): the maximum value of all points in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  max_over_time: (token, query) => {
    if (query.stream) {
      return reg.maxOverTime.viaStream(token, query)
    }
    return reg.maxOverTime.viaRequest(token, query)
  },
  /**
     * minOverTime(unwrapped-range): the minimum value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  min_over_time: (token, query) => {
    if (query.stream) {
      return reg.minOverTime.viaStream(token, query)
    }
    return reg.minOverTime.viaRequest(token, query)
  },
  /**
     * first_over_time(unwrapped-range): the first value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  first_over_time: (token, query) => {
    if (query.stream) {
      return reg.first_over_time.viaStream(token, query)
    }
    return reg.first_over_time.viaRequest(token, query)
  },
  /**
     * lastOverTime(unwrapped-range): the last value of all points in the specified interval
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  last_over_time: (token, query) => {
    if (query.stream) {
      return reg.lastOverTime.viaStream(token, query)
    }
    return reg.lastOverTime.viaRequest(token, query)
  },
  /**
     * stdvarOverTime(unwrapped-range): the population standard variance of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  stdvar_over_time: (token, query) => {
    if (query.stream) {
      return reg.stdvarOverTime.viaStream(token, query)
    }
    return reg.stdvarOverTime.viaRequest(token, query)
  },
  /**
     * stddevOverTime(unwrapped-range): the population standard deviation of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  stddev_over_time: (token, query) => {
    if (query.stream) {
      return reg.stddevOverTime.viaStream(token, query)
    }
    return reg.stddevOverTime.viaRequest(token, query)
  },
  /**
     * quantileOverTime(scalar,unwrapped-range): the φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  quantile_over_time: (token, query) => {
    if (query.stream) {
      return reg.quantileOverTime.viaStream(token, query)
    }
    return reg.quantileOverTime.viaRequest(token, query)
  },
  /**
     * absentOverTime(unwrapped-range): returns an empty vector if the range vector passed to it has any elements and a 1-element vector with the value 1 if the range vector passed to it has no elements. (absentOverTime is useful for alerting on when no time series and logs stream exist for label combination for a certain amount of time.)
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
  absent_over_time: (token, query) => {
    if (query.stream) {
      return reg.absentOverTime.viaStream(token, query)
    }
    return reg.absentOverTime.viaRequest(token, query)
  },

  ...getPlugins('unwrap_registry', (plugin) => {
    return (token, query) => {
      return reg.applyViaStream(
        token,
        query,
        plugin.run,
        plugin.approx,
        false,
        'by_without_unwrap'
      )
    }
  })
}
