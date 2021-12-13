const transpiler = require('../../parser/transpiler')
const {
  dropAlertViews,
  createAlertViews,
  incAlertMark,
  dropOutdatedParts,
  getAlertRules, getLastCheck, deleteAlertRule, deleteGroup, getAlertGroups, getAlerts, putAlertRule
} = require('./clickhouse_alerting')
const { samplesTableName } = require('../utils')
const { durationToMs, parseLabels } = require('../../common')
const { alert } = require('./alertmanager')

/**
 *
 * @param namespace {string}
 * @param group {alerting.group}
 * @returns {Promise<void>}
 */
module.exports.setGroup = async (namespace, group) => {
  /** @type {alerting.rule[]} */
  const rules = group.rules || []
  const rulesToAdd = alerts[namespace] && alerts[namespace][group.name]
    ? rules.filter(r => !alerts[namespace][group.name].rules[r.alert])
    : rules
  const rulesToDelete = alerts[namespace] && alerts[namespace][group.name]
    ? Object.keys(alerts[namespace][group.name].rules)
      .filter(k => !rules.some(r => r.alert === k))
      .map(k => alerts[namespace][group.name].rules[k])
    : []
  const rulesToUpdate = alerts[namespace] && alerts[namespace][group.name]
    ? rules
      .filter(r => alerts[namespace][group.name].rules[r.alert])
      .map(r => [alerts[namespace][group.name].rules[r.alert], r])
    : []
  for (const rul of rulesToAdd) {
    const w = new AlertWatcher(namespace, group, rul)
    await w.init()
    w.run()
    rul._watcher = w
    await putAlertRule(namespace, group, rul)
    addRule(namespace, group, rul)
  }
  for (const rul of rulesToDelete) {
    const w = rul._watcher
    await w.drop()
    await deleteAlertRule(namespace, group.name, rul.alert)
    delRule(namespace, group.name, rul.alert)
  }
  for (const [_old, _new] of rulesToUpdate) {
    const w = _old._watcher
    w.stop()
    await w.edit(group, _new)
    w.run()
    _new._watcher = w
    await putAlertRule(namespace, group, _old)
    addRule(namespace, group, _new)
  }
}

/**
 *
 * @param ns {string}
 * @param group {alerting.group}
 * @param rule {alerting.rule}
 */
const addRule = (ns, group, rule) => {
  alerts[ns] = alerts[ns] || {}
  alerts[ns][group.name] = alerts[ns][group.name] || {}
  alerts[ns][group.name] = {
    interval: group.interval,
    name: group.name,
    rules: alerts[ns][group.name].rules || {}
  }
  alerts[ns][group.name].rules[rule.alert] = rule
}

/**
 * @param ns {string}
 * @param group {string}
 * @param rule {string}
 */
const delRule = (ns, group, rule) => {
  if (!alerts[ns] || !alerts[ns][group] || !alerts[ns][group].rules[rule]) {
    return
  }
  delete alerts[ns][group].rules[rule]
  if (!Object.keys(alerts[ns][group].rules).length) {
    delete alerts[ns][group]
  }
  if (!Object.keys(alerts[ns]).length) {
    delete alerts[ns]
  }
}

/**
 *
 * @returns {Object<string, Object<string, alerting.objGroup>> } namespace
 */
module.exports.getAll = () => {
  return alerts
}

/**
 *
 * @param ns {string}
 * @returns {Object<string, alerting.objGroup>} namespace
 */
module.exports.getNs = (ns) => {
  return alerts[ns]
}

/**
 *
 * @param ns {string}
 * @param grp {string}
 * @returns {alerting.objGroup | undefined} group
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
    w.stop()
    await w.drop()
    await deleteAlertRule(ns, grp, rul.alert)
  }
  await deleteGroup(ns, grp)
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
  for (const ns of Object.values(alerts)) {
    for (const group of Object.values(ns)) {
      for (const rule of Object.values(group.rules)) {
        rule._watcher && rule._watcher.stop()
      }
    }
  }
  alerts = {}
}

module.exports.startAlerting = async () => {
  const rules = await getAlertRules()
  const groups = await getAlertGroups()
  for (const rule of rules) {
    const group = groups.find(g =>
      g.name.ns === rule.name.ns &&
      g.name.group === rule.name.group
    )
    if (!group) {
      console.log(`Not found group for rule ${JSON.stringify(rule)}`)
      continue
    }
    const w = new AlertWatcher(rule.name.ns, group.group, rule.rule)
    w.run()
    rule.rule._watcher = w
    addRule(rule.name.ns, group.group, rule.rule)
  }
}

/**
 *
 * @type {Object<string, Object<string, alerting.objGroup>>}
 */
let alerts = {}

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
    if (this.rule.expr !== rule.expr) {
      this.rule = rule
      this.group = group
      await this._dropViews()
      await this._createViews()
    } else {
      this.rule = rule
      this.group = group
    }
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

  async check () {
    if (typeof this.lastCheck === 'undefined') {
      this.lastCheck = await getLastCheck(this.nsName, this.group.name, this.rule.alert)
    }
    if (Date.now() - this.lastCheck < durationToMs(this.group.interval)) {
      return
    }
    this.lastCheck = await this._checkViews()
  }

  _dropViews () {
    return dropAlertViews(this.nsName, this.group.name, this.rule.alert)
  }

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
    if (lastAlert) {
      const self = this
      console.log('POSTING ' + lastAlert.length)
      await alert(self.rule.alert, lastAlert.map(e => {
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
    await dropOutdatedParts(this.nsName, this.group.name, this.rule.alert, mark)
    return newMark
  }
}
