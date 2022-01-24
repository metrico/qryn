const { transpile } = require('../../../../parser/transpiler')
const CallbackLogAlertWatcher = require('./callbackLogAlertWatcher')
const CallbackTimeSeriesAlertWatcher = require('./callbackTimeSeriesAlertWatcher')
const CallbackCliqlAlertWatcher = require('./callbackCliqlAlertWatcher')
const MVAlertWatcher = require('./MVAlertWatcher')
const { parseCliQL } = require('../../../cliql')
/**
 * @param nsName {string}
 * @param group {alerting.group | alerting.objGroup}
 * @param rule {alerting.rule}
 * @param client {AlertingClient}
 * @returns {AlertWatcher}
 */
module.exports = (nsName, group, rule, client) => {
  const cliQ = parseCliQL(rule.expr)
  if (cliQ) {
    return new CallbackCliqlAlertWatcher(nsName, group, rule, client)
  }
  const q = transpile({
    query: rule.expr,
    limit: 1000,
    start: 0,
    step: 1
  })
  if (q.matrix) {
    return new CallbackTimeSeriesAlertWatcher(nsName, group, rule, client)
  }
  if (q.stream && q.stream.length) {
    return new CallbackLogAlertWatcher(nsName, group, rule, client)
  }
  return new MVAlertWatcher(nsName, group, rule, client)
}
