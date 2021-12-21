const {
  getLastCheck,
  dropAlertViews
} = require('../../clickhouse_alerting')
const { durationToMs, parseLabels } = require('../../../../common')
const { alert } = require('../alertmanager')

class AlertWatcher {
  /**
   *
   * @param nsName {string}
   * @param group {alerting.group | alerting.objGroup}
   * @param rule {alerting.rule}
   */
  constructor (nsName, group, rule) {
    this.nsName = nsName
    this.group = group
    this.rule = rule
  }

  async init () {
    await this._createViews()
  }

  /**
   * @param group {alerting.group | alerting.objGroup}
   * @param rule {alerting.rule}
   * @returns {Promise<void>}
   */
  async edit (group, rule) {
    this.rule = rule
    this.group = group
  }

  async drop () {
    this.stop()
    await this._dropViews()
  }

  stop () {
    if (this.interval) {
      clearInterval(this.interval)
      this.interval = undefined
    }
  }

  run () {
    if (this.interval) {
      return
    }
    const self = this
    this.interval = setInterval(() => {
      self.check().catch(console.log)
    }, 10000)
  }

  async _loadLastCheck () {
    this.lastCheck = await getLastCheck(this.nsName, this.group.name, this.rule.alert)
  }

  async check () {
    if (typeof this.lastCheck === 'undefined') {
      await this._loadLastCheck()
    }
    if (Date.now() - this.lastCheck < durationToMs(this.group.interval)) {
      return
    }
    this.lastCheck = await this._checkViews()
  }

  _dropViews () {
    return dropAlertViews(this.nsName, this.group.name, this.rule.alert)
  }

  /**
   *
   * @param alerts {{
   * labels: Object<string, string>,
   * extra_labels: Object<string, string>,
   * string: string
   * }[]}
   * @returns {Promise<void>}
   */
  async sendTextAlerts (alerts) {
    if (!alerts || !alerts.length) {
      return
    }
    const self = this
    console.log('POSTING ' + alerts.length)
    await alert(self.rule.alert, alerts.map(e => {
      const labels = e.extra_labels
        ? { ...parseLabels(e.labels), ...parseLabels(e.extra_labels) }
        : parseLabels(e.labels)
      return {
        labels: {
          ...(self.rule.labels),
          ...(labels)
        },
        annotations: self.rule.annotations,
        message: e.string
      }
    }))
  }
}
module.exports = AlertWatcher
