const transpiler = require('../../parser/transpiler')
const { dropAlertViews, createAlertViews, incAlertMark, dropOutdatedParts, getLastAlert, getAlertRules } = require('./clickhouse')
const { samplesTableName } = require('../utils')

module.exports.setGroup = async (namespace, group) => {
  const rulesToAdd = alerts[namespace] && alerts[namespace][group.name]
    ? group.rules.filter(r => !alerts[namespace][group.name][r.alert])
    : group.rules
  const rulesToDelete = alerts[namespace] && alerts[namespace][group.name]
    ? Object.keys(alerts[namespace][group.name])
      .filter(k => !group.alerts.some(r => r.alert === k))
      .map(k => alerts[namespace][group.name][k])
    : []
  const rulesToUpdate = alerts[namespace] && alerts[namespace][group.name]
    ? group.rules
      .filter(r => alerts[namespace][group.name][r.alert])
      .map(r => [alerts[namespace][group.name][r.alert], r])
    : []
  for (const rul of rulesToAdd) {
    const w = new AlertWatcher(rul)
    await w.run()
    rul._watcher = w
  }
  for (const rul of rulesToDelete) {
    const w = rul._watcher
    await w.drop()
  }
  for (const [_old, _new] of rulesToUpdate) {
    const w = _old._watcher
    await w.edit(_new)
    _new._watcher = w
  }
  alerts[namespace] = alerts[namespace] || {}
  alerts[namespace][group.name] = group
}

/**
 *
 * @param ns {string}
 * @returns {Object<string, Object<string, {name: string, interval: string, rules: Object}>> } namespace
 */
module.exports.getAll = () => {
  return alerts
}

/**
 *
 * @param ns {string}
 * @returns {Object<string, {name: string, interval: string, rules: Object}>} namespace
 */
module.exports.getNs = (ns) => {
  return alerts[ns]
}

/**
 *
 * @param ns {string}
 * @param grp {string}
 * @returns {{name: string, interval: string, rules: Object} | undefined} group
 */
module.exports.getGroup = (ns, grp) => {
  return alerts[ns] && alerts[ns][grp] ? alerts[ns][grp] : undefined
}

/**
 *
 * @param ns {string}
 * @param grp {string}
 * @returns {Promise<void>}
 */
module.exports.dropGroup = async (ns, grp) => {
  if (!alerts[ns] || !alerts[ns][grp]) {
    return
  }
  for (const rul of Object.values(alerts[ns][grp].rules)) {
    const w = rul._watcher
    await w.drop()
  }
  delete alerts[ns][grp]
}

/**
 *
 * @param ns {string}
 * @returns {Promise<void>}
 */
module.exports.dropNs = async (ns) => {
  if (!alerts[ns]) {
    return
  }
  for (const grp of Object.keys(alerts[ns])) {
    await module.exports.dropGroup(ns, grp)
  }
  delete alerts[ns]
}

module.exports.stop = () => {
  Object.values(alerts).forEach(a => a.stop())
  alerts = {}
}

module.exports.startAlerting = async () => {
  const rules = await getAlertRules()
  for (const rule of rules) {
    alerts[rule.name] = new AlertWatcher(rule.name, rule.request, rule.labels)
    await alerts[rule.name].run()
  }
}

/**
 *
 * @type {Object<string, Object<string, {name: string, interval: string, rules: Object}>>}
 */
let alerts = {}

class AlertWatcher {
  constructor (name, request, labels) {
    this.name = name
    this.request = request
    this.labels = labels
  }

  async run () {
    await this._createViews()
    this.interval = setInterval(() => {
      this._checkViews().catch(console.log)
    }, 1000)
  }

  async edit (request, labels) {
    if (this.request !== request) {
      this.request = request
      await this._dropViews()
      await this._createViews()
    }
    this.labels = labels
  }

  async update() {

  }

  async drop () {
    this.stop()
    await this._dropViews()
  }

  stop () {
    if (this.interval) {
      clearInterval(this.interval)
    }
  }

  _dropViews () {
    return dropAlertViews(this.name)
  }

  async _createViews () {
    /**
     *
     * @type {{query: Select, stream: (function(DataStream): DataStream)[]}}
     */
    const query = transpiler.transpileTail({
      query: this.request,
      samplesTable: samplesTableName,
      rawRequest: true,
      suppressTime: true
    })
    query.query.order_expressions = []
    return createAlertViews(this.name, query.query)
  }

  async _checkViews () {
    const mark = await incAlertMark(this.name)
    const lastAlert = await getLastAlert(this.name, mark)
    console.log(lastAlert)
    await dropOutdatedParts(this.name, mark)
  }
}
