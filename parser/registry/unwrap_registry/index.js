const reg = require('./unwrap_registry')
const { getPlugins, hasStream } = require('../common')

module.exports = {
  /**
     * rate(unwrapped-range): calculates per second rate of all values in the specified interval.
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  rate: (token, query) => {
    if (hasStream(query)) {
      return reg.rate.viaStream(token, query)
    }
    return reg.rate.viaRequest(token, query)
  },
  /**
     * sumOverTime(unwrapped-range): the sum of all values in the specified interval.
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  sum_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.sumOverTime.viaStream(token, query)
    }
    return reg.sumOverTime.viaRequest(token, query)
  },
  /**
     * avgOverTime(unwrapped-range): the average value of all points in the specified interval.
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  avg_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.avgOverTime.viaStream(token, query)
    }
    return reg.avgOverTime.viaRequest(token, query)
  },
  /**
     * maxOverTime(unwrapped-range): the maximum value of all points in the specified interval.
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  max_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.maxOverTime.viaStream(token, query)
    }
    return reg.maxOverTime.viaRequest(token, query)
  },
  /**
     * minOverTime(unwrapped-range): the minimum value of all points in the specified interval
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  min_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.minOverTime.viaStream(token, query)
    }
    return reg.minOverTime.viaRequest(token, query)
  },
  /**
     * first_over_time(unwrapped-range): the first value of all points in the specified interval
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  first_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.firstOverTime.viaStream(token, query)
    }
    return reg.firstOverTime.viaRequest(token, query)
  },
  /**
     * lastOverTime(unwrapped-range): the last value of all points in the specified interval
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  last_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.lastOverTime.viaStream(token, query)
    }
    return reg.lastOverTime.viaRequest(token, query)
  },
  /**
     * stdvarOverTime(unwrapped-range): the population standard variance of the values in the specified interval.
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  stdvar_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.stdvarOverTime.viaStream(token, query)
    }
    return reg.stdvarOverTime.viaRequest(token, query)
  },
  /**
     * stddevOverTime(unwrapped-range): the population standard deviation of the values in the specified interval.
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  stddev_over_time: (token, query) => {
    if (hasStream(query)) {
      return reg.stddevOverTime.viaStream(token, query)
    }
    return reg.stddevOverTime.viaRequest(token, query)
  },
  /**
     * absentOverTime(unwrapped-range): returns an empty vector if the range vector passed to it has any elements and a 1-element vector with the value 1 if the range vector passed to it has no elements. (absentOverTime is useful for alerting on when no time series and logs stream exist for label combination for a certain amount of time.)
     * @param token {Token}
     * @param query {Select}
     * @returns {Select}
     */
  absent_over_time: (token, query) => {
    if (hasStream(query)) {
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
