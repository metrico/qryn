const transpiler = require('../../../../parser/transpiler')
const { getClickhouseStream, preprocessStream } = require('../../clickhouse')
const AlertWatcher = require('./alertWatcher')

class CallbackLogAlertWatcher extends AlertWatcher {
  _dropViews () {
    return this.client.dropAlertViews(this.nsName, this.group.name, this.rule.alert)
  }

  async _createViews () {
    return this.client.createMarksTable(this.nsName, this.group.name, this.rule.alert)
  }

  /**
   * @return {Promise<number>}
   * @private
   */
  async _checkViews () {
    this.lastCheck = this.lastCheck || Date.now()
    const lastMark = this.lastCheck
    let newMark = 0
    const query = transpiler.transpile({
      query: this.rule.expr,
      rawRequest: true,
      start: `${lastMark}000000`,
      end: newMark + '000000',
      limit: 1000
    })
    const _stream = await getClickhouseStream(query)
    const stream = preprocessStream(_stream, query.stream)
    let alerts = []
    for await (const e of stream.toGenerator()()) {
      if (!e || !e.labels) {
        continue
      }
      newMark = Math.max(newMark, parseInt(e.timestamp_ms))
      alerts.push(e)
      if (alerts.length > 100) {
        await this.sendTextAlerts(alerts)
        alerts = []
      }
    }
    await this.sendTextAlerts(alerts)
    alerts = []
    newMark = newMark || lastMark
    const marks = await this.client.incAlertMark(this.nsName, this.group.name, this.rule.alert, newMark)
    return marks[1]
  }
}

module.exports = CallbackLogAlertWatcher
