const { transpile } = require('../../../../parser/transpiler')
const CallbackLogAlertWatcher = require('./callbackLogAlertWatcher')
const CallbackTimeSeriesAlertWatcher = require('./callbackTimeSeriesAlertWatcher')
const CallbackCliqlAlertWatcher = require('./callbackCliqlAlertWatcher')
const MVAlertWatcher = require('./MVAlertWatcher')
const { parseCliQL } = require('../../../cliql')
const {clusterName} = require('../../../../common')
/**
 * @param nsName {string}
 * @param group {alerting.group | alerting.objGroup}
 * @param rule {alerting.rule}
 * @returns {AlertWatcher}
 */
module.exports = (nsName, group, rule) => {
  const cliQ = parseCliQL(rule.expr)
  if (cliQ) {
    return new CallbackCliqlAlertWatcher(nsName, group, rule)
  }
  const q = transpile({
    query: rule.expr,
    limit: 1000,
    start: 0,
    step: 1
  })
  if (q.matrix) {
    return new CallbackTimeSeriesAlertWatcher(nsName, group, rule)
  }
  if ((q.stream && q.stream.length) || clusterName) {
    return new CallbackLogAlertWatcher(nsName, group, rule)
  }
  return new MVAlertWatcher(nsName, group, rule)
}
