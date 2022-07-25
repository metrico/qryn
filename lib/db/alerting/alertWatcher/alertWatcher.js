const {
  getLastCheck,
  dropAlertViews
} = require('../../clickhouse_alerting')
const { durationToMs, parseLabels, errors } = require('../../../../common')
const { alert } = require('../alertmanager')
const compiler = require('../../../../parser/bnf')
const logger = require('../../../logger')

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
      self.check().catch(e => logger.error(e))
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
    } catch (err) {
      logger.error({ err })
      this.health = 'error'
      this.lastError = err.message
    }
  }

  _dropViews () {
    return dropAlertViews(this.nsName, this.group.name, this.rule.alert)
  }

  assertExpr () {
    compiler.ParseScript(this.rule.expr.trim())
    for (const lbl of Object.keys(this.rule.labels || {})) {
      if (!lbl.match(/[a-zA-Z_][a-zA-Z_0-9]*/)) {
        throw new errors.QrynError(400, 'Bad request', `Label ${lbl} is invalid`)
      }
    }
    for (const ann of Object.keys(this.rule.annotations || {})) {
      if (!ann.match(/[a-zA-Z_][a-zA-Z_0-9]*/)) {
        throw new errors.QrynError(400, 'Bad request', `Annotation ${ann} is invalid`)
      }
    }
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
    this.lastAlert = _alerts[_alerts.length - 1]
    this.firingSince = Date.now()
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
