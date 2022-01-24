const transpiler = require('../../../../parser/transpiler')
const { getClickhouseStream, preprocessStream } = require('../../clickhouse')
const AlertWatcher = require('./alertWatcher')
const { sharedParamNames } = require('../../../../parser/registry/common')
const { durationToMs } = require('../../../../common')

class CallbackTimeSeriesAlertWatcher extends AlertWatcher {
  _dropViews () {
    return this.client.dropAlertViews(this.nsName, this.group.name, this.rule.alert)
  }

  async _createViews () {
    return this.client.createMarksTable(this.nsName, this.group.name, this.rule.alert)
  }

  async _loadLastCheck () {
    const last = parseInt(await this.client.getLastCheck(this.nsName, this.group.name, this.rule.alert))
    this.active = last % 2
    this.lastCheck = Math.floor(last / 2)
    this.activeSince = parseInt(await this.client.getLastCheck(this.nsName, this.group.name, this.rule.alert, 1))
  }

  /**
   * @return {Promise<number>}
   * @private
   */
  async _checkViews () {
    this.lastCheck = this.lastCheck || Date.now()
    const lastMark = this.lastCheck
    const newMark = Date.now()
    const query = transpiler.transpile({
      query: this.rule.expr,
      rawRequest: true,
      start: `${lastMark}000000`,
      end: Date.now() + '000000',
      limit: 1000,
      rawQuery: true
    })
    const from = newMark - query.query.ctx.duration
    query.query.getParam('timestamp_shift').set(from)
    query.query.getParam(sharedParamNames.from).set(from)
    query.query.getParam(sharedParamNames.to).set(newMark)
    const _stream = await getClickhouseStream({ query: query.query.toString() })
    const stream = preprocessStream(_stream, query.stream)
    let active = false
    const activeRows = []
    for await (const e of stream.toGenerator()()) {
      if (!e || !e.labels) {
        continue
      }
      active = true
      activeRows.push({ ...e, string: e.value })
    }
    if (active && !this.active) {
      this.activeSince = newMark
    }
    this.active = active
    const durationMS = durationToMs(this.rule.for)
    if (this.active && newMark - durationMS >= this.activeSince) {
      await this.sendTextAlerts(activeRows)
    } else {
      await this.sendTextAlerts(undefined)
    }
    await this.client.incAlertMark(this.nsName, this.group.name, this.rule.alert,
      (newMark * 2) + (active ? 1 : 0)
    )
    await this.client.incAlertMark(this.nsName, this.group.name, this.rule.alert,
      this.activeSince, 1
    )
    return newMark
  }
}

module.exports = CallbackTimeSeriesAlertWatcher
