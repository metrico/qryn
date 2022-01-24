/**
 *  ALERT RULES
 */

const axios = require('axios')
const { DATABASE_NAME } = require('../utils')
const UTILS = require('../utils')
const { getClickhouseUrl, client } = require('./clickhouse')
const Sql = require('@cloki/clickhouse-sql')

module.exports.AlertingClient = class AlertingClient extends client {
  /**
   * @param url {string | Object | CLokiClient}
   */
  constructor (url) {
    super(url instanceof client ? url.getClickhouseUrl().toString() : url)
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param name {string}
   * @returns {Promise<undefined|alerting.rule>}
   */
  async getAlertRule (ns, group, name) {
    const fp = this.getRuleFP(ns, group, name)
    const mark = Math.random()
    const res = await this.rawRequest(
      'SELECT fingerprint, argMax(name, inserted_at) as name, argMax(value, inserted_at) as value ' +
      `FROM ${DATABASE_NAME()}.settings ` +
      `WHERE fingerprint = ${fp} AND ${mark} == ${mark} ` +
      'GROUP BY fingerprint ' +
      'HAVING name != \'\' ' +
      'FORMAT JSON'
    )
    if (!res.data.data.length) {
      return undefined
    }
    const rule = JSON.parse(res.data.data[0].value)
    return rule
  }

  /**
   *
   * @param namespace {string}
   * @param group {alerting.group}
   * @param rule {alerting.rule}
   * @returns {Promise<undefined>}
   */
  async putAlertRule (namespace, group, rule) {
    const ruleName = JSON.stringify({ type: 'alert_rule', ns: namespace, group: group.name, rule: rule.alert })
    const ruleFp = this.getRuleFP(namespace, group.name, rule.alert)
    const ruleVal = { ...rule }
    delete ruleVal._watcher
    const groupName = JSON.stringify({ type: 'alert_group', ns: namespace, group: group.name })
    const groupFp = this.getGroupFp(namespace, group.name)
    const groupVal = JSON.stringify({ name: group.name, interval: group.interval })
    await this.rawRequest(
      `INSERT INTO ${DATABASE_NAME()}.settings (fingerprint, type, name, value, inserted_at) FORMAT JSONEachRow   ` +
      JSON.stringify({
        fingerprint: ruleFp,
        type: 'alert_rule',
        name: ruleName,
        value: JSON.stringify(ruleVal),
        inserted_at: Date.now() * 1000000
      }) + '\n' +
      JSON.stringify({
        fingerprint: groupFp,
        type: 'alert_group',
        name: groupName,
        value: groupVal,
        inserted_at: Date.now() * 1000000
      })
    )
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @param id {number}
   * @return {Promise<number>}
   */
  async getLastCheck (ns, group, rule, id) {
    const fp = this.getRuleFP(ns, group, rule)
    id = id || 0
    const resp = await this.rawRequest(
      `SELECT max(mark) as maxmark
       FROM ${DATABASE_NAME()}._alert_view_${fp} _mark WHERE id = ${id} FORMAT JSON`
    )
    if (!resp.data.data[0]) {
      return 0
    }
    return resp.data.data[0].maxmark
  }

  /**
   *
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @param timeMs {number}
   * @returns {Promise<void>}
   */
  async activeSince (ns, group, rule, timeMs) {
    const fp = this.getRuleFP(ns, group, rule)
    await this.rawRequest(
      `INSERT INTO ${DATABASE_NAME()}._alert_view_${fp}_mark (id, mark) (1, ${timeMs})`
    )
  }

  /**
   * @see alerting.d.ts
   * @param limit {number | undefined}
   * @param offset {number | undefined}
   * @returns {Promise<[{rule: alerting.rule,name: alerting.ruleName}]>}
   */
  async getAlertRules (limit, offset) {
    const _limit = limit ? `LIMIT ${limit}` : ''
    const _offset = offset ? `OFFSET ${offset}` : ''
    const mark = Math.random()
    const res = await this.rawRequest(
      'SELECT fingerprint, argMax(name, inserted_at) as name, argMax(value, inserted_at) as value ' +
      `FROM ${DATABASE_NAME()}.settings ` +
      `WHERE type == 'alert_rule' AND  ${mark} == ${mark} ` +
      `GROUP BY fingerprint HAVING name != '' ORDER BY name ${_limit} ${_offset} FORMAT JSON`)
    return res.data.data.map(e => {
      return { rule: JSON.parse(e.value), name: JSON.parse(e.name) }
    })
  }

  /**
   *
   * @param limit {number | undefined}
   * @param offset {number | undefined}
   * @returns {Promise<[{group: alerting.group, name: alerting.groupName}]>}
   */
  async getAlertGroups (limit, offset) {
    const _limit = limit ? `LIMIT ${limit}` : ''
    const _offset = offset ? `OFFSET ${offset}` : ''
    const mark = Math.random()
    const res = await this.rawRequest(
      'SELECT fingerprint, argMax(name, inserted_at) as name, argMax(value, inserted_at) as value ' +
      `FROM ${DATABASE_NAME()}.settings ` +
      `WHERE type == 'alert_group' AND  ${mark} == ${mark} ` +
      `GROUP BY fingerprint HAVING name != '' ORDER BY name ${_limit} ${_offset} FORMAT JSON`)
    return res.data.data.map(e => {
      return { group: JSON.parse(e.value), name: JSON.parse(e.name) }
    })
  }

  /**
   * @returns {Promise<number>}
   */
  async getAlertRulesCount () {
    const mark = Math.random()
    const res = await this.rawRequest(
      'SELECT COUNT(1) as count FROM (SELECT fingerprint ' +
      `FROM ${DATABASE_NAME()}.settings ` +
      `WHERE type=\'alert_rule\' AND ${mark} == ${mark} ` +
      'GROUP BY fingerprint ' +
      'HAVING argMax(name, inserted_at) != \'\') FORMAT JSON')
    return parseInt(res.data.data[0].count)
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @returns {Promise<undefined>}
   */
  async deleteAlertRule (ns, group, rule) {
    const fp = this.getRuleFP(ns, group, rule)
    await this.rawRequest(
      `INSERT INTO ${DATABASE_NAME()}.settings (fingerprint, type, name, value, inserted_at) FORMAT JSONEachRow  ` +
      JSON.stringify({ fingerprint: fp, type: 'alert_rule', name: '', value: '', inserted_at: Date.now() })
    )
    await this.rawRequest(
      `ALTER TABLE ${DATABASE_NAME()}.settings DELETE WHERE fingerprint=${fp} AND inserted_at <= now64(9, 'UTC')`
    )
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @return {Promise<void>}
   */
  async deleteGroup (ns, group) {
    const fp = this.getGroupFp(ns, group)
    await this.rawRequest(
      `INSERT INTO ${DATABASE_NAME()}.settings (fingerprint, type, name, value, inserted_at) FORMAT JSONEachRow  ` +
      JSON.stringify({ fingerprint: fp, type: 'alert_group', name: '', value: '', inserted_at: Date.now() })
    )
    await this.rawRequest(
      `ALTER TABLE ${DATABASE_NAME()}.settings DELETE WHERE fingerprint=${fp} AND inserted_at <= now64(9, 'UTC')`
    )
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @returns {Promise<void>}
   */
  async dropAlertViews (ns, group, rule) {
    const fp = this.getRuleFP(ns, group, rule)
    await this.rawRequest(
      `DROP VIEW IF EXISTS ${DATABASE_NAME()}._alert_view_${fp}`)
    await this.rawRequest(
      `DROP TABLE IF EXISTS ${DATABASE_NAME()}._alert_view_${fp}_mark`)
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @returns {Promise<void>}
   */
  async createMarksTable (ns, group, rule) {
    const fp = this.getRuleFP(ns, group, rule)
    await this.rawRequest(
      `CREATE TABLE IF NOT EXISTS ${DATABASE_NAME()}._alert_view_${fp}_mark ` +
      '(id UInt8 default 0,mark UInt64, inserted_at DateTime default now()) ' +
      'ENGINE ReplacingMergeTree(mark) ORDER BY id')
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @param request {Select}
   * @returns {Promise<void>}
   */
  async createAlertViews (ns, group, rule, request) {
    const fp = this.getRuleFP(ns, group, rule)
    request.select(
      [
        new Sql.Raw(`coalesce((SELECT max(mark) FROM ${DATABASE_NAME()}._alert_view_${fp}_mark WHERE id = 0), 0)`),
        'mark'
      ]
    )
    request.withs.str_sel.inline = true
    const strRequest = request.toString()
    await this.createMarksTable(ns, group, rule, request)
    await this.rawRequest(
      `INSERT INTO ${DATABASE_NAME()}._alert_view_${fp}_mark (mark)
       VALUES (${Date.now()})`)
    await this.rawRequest(
      `CREATE MATERIALIZED VIEW IF NOT EXISTS ${DATABASE_NAME()}._alert_view_${fp} ` +
      `ENGINE=MergeTree() ORDER BY timestamp_ms PARTITION BY mark AS (${strRequest})`)
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @returns {Promise<number>}
   */
  async getLastMark (ns, group, rule) {
    const fp = this.getRuleFP(ns, group, rule)
    const mark = await this.rawRequest(
      `SELECT max(mark) as mark
       FROM ${DATABASE_NAME()}._alert_view_${fp} _mark WHERE id = 0 FORMAT JSON`)
    return parseInt(mark.data.data[0].mark)
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @param newMark {number}
   * @param id {number}
   * @return {Promise<[number, number]>} old mark and new mark
   */
  async incAlertMark (ns, group, rule, newMark, id) {
    const fp = this.getRuleFP(ns, group, rule)
    const mark = await this.getLastMark(ns, group, rule)
    newMark = newMark || Date.now()
    id = id || 0
    await this.rawRequest(
      `INSERT INTO ${DATABASE_NAME()}._alert_view_${fp}_mark (mark, id)
       VALUES (${newMark}, ${id})`)
    return [mark, newMark]
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @param mark {number}
   * @return {Promise<*>}
   */
  async getAlerts (ns, group, rule, mark) {
    const fp = this.getRuleFP(ns, group, rule)
    const lastMsg = await this.rawRequest(
      `SELECT *
       FROM ${DATABASE_NAME()}._alert_view_${fp}
       WHERE mark <= ${mark}
       ORDER BY timestamp_ms DESC FORMAT JSON`)
    if (!lastMsg.data.data || !lastMsg.data.data.length) {
      return undefined
    }
    return lastMsg.data.data
  }

  /**
   *
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @param mark {number}
   * @returns {Promise<void>}
   */
  async dropOutdatedParts (ns, group, rule, mark) {
    const fp = this.getRuleFP(ns, group, rule)
    const partitions = await axios.post(getClickhouseUrl(),
      `SELECT DISTINCT mark
       FROM ${DATABASE_NAME()}._alert_view_${fp}
       WHERE mark <= ${mark} FORMAT JSON`)
    if (!partitions.data || !partitions.data.data || !partitions.data.data.length) {
      return
    }
    for (const partid of partitions.data.data) {
      await axios.post(getClickhouseUrl(),
        `ALTER TABLE ${DATABASE_NAME()}._alert_view_${fp} DROP PARTITION tuple(${partid.mark})`)
    }
  }

  /**
   * @param ns {string}
   * @param group {string}
   * @param rule {string}
   * @returns {number}
   */
  getRuleFP (ns, group, rule) {
    const ruleName = JSON.stringify({ type: 'alert_rule', ns: ns, group: group, rule: rule })
    const ruleFp = UTILS.fingerPrint(ruleName)
    return ruleFp
  }

  /**
   * @param ns {string}
   * @param group {string}
   */
  getGroupFp (ns, group) {
    const groupName = JSON.stringify({ type: 'alert_group', ns: ns, group: group })
    const groupFp = UTILS.fingerPrint(groupName)
    return groupFp
  }
}
