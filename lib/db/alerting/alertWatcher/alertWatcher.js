const {
  getLastCheck,
  dropAlertViews
} = require('../../clickhouse_alerting')
const { durationToMs, parseLabels } = require('../../../../common')
const { alert } = require('../alertmanager')
const compiler = require('../../../../parser/bnf')

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
    try {
      if (typeof this.lastCheck === 'undefined') {
        await this._loadLastCheck()
      }
      if (Date.now() - this.lastCheck < durationToMs(this.group.interval)) {
        return
      }
      this.lastCheck = await this._checkViews()
      this.health = 'ok'
      this.lastError = ''
    } catch (e) {
      console.error(e.message)
      console.error(e.stack)
      console.error(e.data)
      this.health = 'error'
      this.lastError = e.message
    }
  }

  _dropViews () {
    return dropAlertViews(this.nsName, this.group.name, this.rule.alert)
  }

  assertExpr () {
    compiler.ParseScript(this.rule.expr.trim())
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
      this.state = 'normal'
      this.lastAlert = null
      return
    }
    const self = this
    this.state = 'firing'
    const _alerts = alerts.map(e => {
      const labels = e.extra_labels
        ? { ...parseLabels(e.labels), ...parseLabels(e.extra_labels) }
        : parseLabels(e.labels)
      return {
        labels: {
          ...(self.rule.labels || {}),
          ...(labels)
        },
        annotations: self.rule.annotations || {},
        message: e.string.toString()
      }
    })
    console.log(_alerts)
    this.lastAlert = _alerts[_alerts.length - 1]
    this.firingSince = Date.now()
    console.log('POSTING ' + alerts.length)
    await alert(self.rule.alert, _alerts)
  }

  getLastAlert () {
    return this.state === 'firing'
      ? {
          ...this.lastAlert,
          activeAt: this.firingSince,
          state: 'firing'
        }
      : undefined
  }
}
module.exports = AlertWatcher
