const {
  createAlertViews,
  incAlertMark,
  getAlerts,
  dropOutdatedParts
} = require('../../clickhouse_alerting')
const transpiler = require('../../../../parser/transpiler')
const { samplesTableName } = require('../../../utils')
const AlertWatcher = require('./alertWatcher')

class MVAlertWatcher extends AlertWatcher {
  async _createViews () {
    /** @type {{query: Select, stream: (function(DataStream): DataStream)[]}} */
    const query = transpiler.transpileTail({
      query: this.rule.expr,
      samplesTable: samplesTableName,
      rawRequest: true,
      suppressTime: true
    })
    if (query.stream && query.stream.length) {
      throw new Error(`Query ${this.rule.expr} is not supported`)
    }
    query.query.order_expressions = []
    return createAlertViews(this.nsName, this.group.name, this.rule.alert, query.query)
  }

  /**
   * @return {Promise<number>}
   * @private
   */
  async _checkViews () {
    const [mark, newMark] = await incAlertMark(this.nsName, this.group.name, this.rule.alert)
    const lastAlert = await getAlerts(this.nsName, this.group.name, this.rule.alert, mark)
    await this.sendTextAlerts(lastAlert)
    await dropOutdatedParts(this.nsName, this.group.name, this.rule.alert, mark)
    return newMark
  }
}

module.exports = MVAlertWatcher
