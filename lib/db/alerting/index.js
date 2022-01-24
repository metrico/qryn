const factory = require('./alertWatcher')
const { getClient } = require('../../multitenancy')
const AlertingClient = require('../clickhouse_alerting').AlertingClient
let enabled = false
/**
 *
 * @param namespace {string}
 * @param group {alerting.group}
 * @param client {AlertingClient}
 * @returns {Promise<void>}
 */
module.exports.setGroup = async (namespace, group, client) => {
  for (const r of group.rules || []) {
    r.labels = r.labels || {}
    r.annotations = r.annotations || {}
  }
  /** @type {alerting.rule[]} */
  const rules = group.rules || []
  const alerts = getTenant(client.getClickhouseUrl())
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
    const w = factory(namespace, group, rul, client)
    w.assertExpr()
    await w.init()
    w.run()
    rul._watcher = w
    await client.putAlertRule(namespace, group, rul)
    addRule(namespace, group, rul, client)
  }
  for (const rul of rulesToDelete) {
    const w = rul._watcher
    await w.drop()
    await client.deleteAlertRule(namespace, group.name, rul.alert)
    delRule(namespace, group.name, rul.alert, client)
  }
  for (const [_old, _new] of rulesToUpdate) {
    if (_old.expr !== _new.expr) {
      const w = _old._watcher
      await w.drop()
      const _w = factory(namespace, group, _new, client)
      _w.assertExpr()
      await _w.init()
      _w.run()
      _new._watcher = _w
      await client.putAlertRule(namespace, group, _new)
      addRule(namespace, group, _new, client)
      continue
    }
    const w = _old._watcher
    w.stop()
    await w.edit(group, _new)
    w.run()
    _new._watcher = w
    await client.putAlertRule(namespace, group, _old)
    addRule(namespace, group, _new, client)
  }
}

/**
 *
 * @param ns {string}
 * @param group {alerting.group}
 * @param rule {alerting.rule}
 * @param client {AlertingClient}
 */
const addRule = (ns, group, rule, client) => {
  const alerts = getTenant(client.getClickhouseUrl())
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
 * @param client {AlertingClient}
 */
const delRule = (ns, group, rule, client) => {
  const alerts = getTenant(client.getClickhouseUrl())
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
 * @param client {AlertingClient}
 * @returns {Object<string, Object<string, alerting.objGroup>> } namespace
 */
module.exports.getAll = (client) => {
  return getTenant(client.getClickhouseUrl())
}

/**
 *
 * @param ns {string}
 * @param client {AlertingClient}
 * @returns {Object<string, alerting.objGroup>} namespace
 */
module.exports.getNs = (ns, client) => {
  return getTenant(client.getClickhouseUrl())[ns]
}

/**
 *
 * @param ns {string}
 * @param grp {string}
 * @param client {AlertingClient}
 * @returns {alerting.objGroup | undefined} group
 */
module.exports.getGroup = (ns, grp, client) => {
  const alerts = getTenant(client.getClickhouseUrl())
  return alerts[ns] && alerts[ns][grp] ? alerts[ns][grp] : undefined
}

/**
 *
 * @param ns {string}
 * @param grp {string}
 * @param client {AlertingClient}
 * @returns {Promise<void>}
 */
module.exports.dropGroup = async (ns, grp, client) => {
  const alerts = getTenant(client.getClickhouseUrl())
  if (!alerts[ns] || !alerts[ns][grp]) {
    return
  }
  for (const rul of Object.values(alerts[ns][grp].rules)) {
    const w = rul._watcher
    w.stop()
    await w.drop()
    await client.deleteAlertRule(ns, grp, rul.alert)
  }
  await client.deleteGroup(ns, grp)
  delete alerts[ns][grp]
}

/**
 *
 * @param ns {string}
 * @param client {AlertingClient}
 * @returns {Promise<void>}
 */
module.exports.dropNs = async (ns, client) => {
  const alerts = getTenant(client.getClickhouseUrl())
  if (!alerts[ns]) {
    return
  }
  for (const grp of Object.keys(alerts[ns])) {
    await module.exports.dropGroup(ns, grp, client)
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

/**
 * @param client {AlertingClient}
 * @returns {Promise<void>}
 */
module.exports.startAlerting = async (client) => {
  const defaultClient = new AlertingClient(await getClient())
  const dbs = await defaultClient.getAllDBs()
  const clients = [defaultClient, ...dbs.map(db => new AlertingClient(db.url))]
  for (const client of clients) {
    const rules = await client.getAlertRules()
    const groups = await client.getAlertGroups()
    for (const rule of rules) {
      rule.labels = rule.labels || {}
      rule.annotations = rule.annotations || {}
      const group = groups.find(g =>
        g.name.ns === rule.name.ns &&
        g.name.group === rule.name.group
      )
      if (!group) {
        console.log(`Not found group for rule ${JSON.stringify(rule)}`)
        continue
      }
      const w = factory(rule.name.ns, group.group, rule.rule, client)
      w.run()
      rule.rule._watcher = w
      addRule(rule.name.ns, group.group, rule.rule, client)
    }
  }
  enabled = true
}

module.exports.isEnabled = () => enabled

/**
 *
 * @type {Object<string, Object<string, Object<string, alerting.objGroup>>>}
 */
let alerts = {}
/**
 *
 * @param url {string}
 * @returns {Object<string, Object<string, alerting.objGroup>>}
 */
const getTenant = (url) => {
  if (!alerts[url.toString()]) {
    alerts[url.toString()] = {}
  }
  return alerts[url.toString()]
}
