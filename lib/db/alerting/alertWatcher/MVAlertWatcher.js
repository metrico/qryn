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
    return this.client.createAlertViews(this.nsName, this.group.name, this.rule.alert, query.query)
  }

  /**
   * @return {Promise<number>}
   * @private
   */
  async _checkViews () {
    const [mark, newMark] = await this.client.incAlertMark(this.nsName, this.group.name, this.rule.alert)
    const lastAlert = await this.client.getAlerts(this.nsName, this.group.name, this.rule.alert, mark)
    await this.sendTextAlerts(lastAlert)
    await this.client.dropOutdatedParts(this.nsName, this.group.name, this.rule.alert, mark)
    return newMark
  }
}

module.exports = MVAlertWatcher
