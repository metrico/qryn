const { scanClickhouse } = require('../../clickhouse')
const CallbackTimeSeriesAlertWatcher = require('./callbackTimeSeriesAlertWatcher')
const { parseCliQL } = require('../../../cliql')

class CallbackCliqlAlertWatcher extends CallbackTimeSeriesAlertWatcher {
  assertExpr () { }
  /**
   * @return {Promise<number>}
   * @private
   */
  async _checkViews () {
    this.lastCheck = this.lastCheck || Date.now()
    const newMark = Date.now()
    const params = parseCliQL(this.rule.expr)
    const from = newMark - parseInt(params.interval) * 1000
    let active = false
    const alerts = []
    await new Promise((resolve, reject) => {
      try {
        scanClickhouse(params, {
          code: () => {},
          send: (data) => {
            if (data.data && data.data.result) {
              for (const metric of data.data.result) {
                for (const val of metric.values) {
                  active = true
                  alerts.push({
                    labels: metric.metric,
                    string: val[1]
                  })
                }
              }
              resolve(alerts)
              return
            }
            reject(new Error(data))
          }
        }, {
          start: from + '000000',
          end: newMark + '000000',
          shift: from
        })
      } catch (e) {
        reject(e)
      }
    })
    await this.sendTextAlerts(alerts)
    await this.client.incAlertMark(this.nsName, this.group.name, this.rule.alert,
      (newMark * 2) + (active ? 1 : 0)
    )
    await this.client.incAlertMark(this.nsName, this.group.name, this.rule.alert,
      this.activeSince, 1
    )
    return newMark
  }
}

module.exports = CallbackCliqlAlertWatcher
