const hb = require('handlebars')
const client = require('../clickhouse')
const logger = require('../../logger')
const {samplesOrderingRule, clusterName} = require('../../../common')
const scripts = require('./scripts')
const getEnv = () => {
  return {
    CLICKHOUSE_DB: 'cloki',
    LABELS_DAYS: 7,
    SAMPLES_DAYS: 7,
    ...process.env
  }
}

/**
 *
 * @param db {string}
 * @returns {Promise<void>}
 */
module.exports.upgrade = async (db) => {
  const scripts = require('./scripts')
  await upgradeSingle(db, 1, scripts.overall)
  await upgradeSingle(db, 2, scripts.traces)
  if (clusterName) {
    await upgradeSingle(db, 3, scripts.overall_dist)
    await upgradeSingle(db, 4, scripts.traces_dist)
  }
}

let isDBCreated = false
/**
 *
 * @param db {string}
 * @param key {number}
 * @param scripts {string[]}
 */
const upgradeSingle = async (db, key, scripts) => {
  const _upgradeRequest = (request, useDefaultDB, updateVer) => {
    return upgradeRequest(db, request, useDefaultDB, updateVer)
  }
  if (!isDBCreated) {
    isDBCreated = true
    await _upgradeRequest('CREATE DATABASE IF NOT EXISTS {{DB}} {{{OnCluster}}}')
    if (clusterName) {
      await _upgradeRequest('CREATE TABLE IF NOT EXISTS {{DB}}._ver {{{OnCluster}}} (k UInt64, ver UInt64) ' +
          'ENGINE={{ReplacingMergeTree}}(ver) ORDER BY k', true)
      await _upgradeRequest('CREATE TABLE IF NOT EXISTS {{DB}}.ver {{{OnCluster}}} (k UInt64, ver UInt64) ' +
          'ENGINE=Distributed(\'{{CLUSTER}}\',\'{{DB}}\', \'_ver\', rand())', true)
    } else {
      await _upgradeRequest('CREATE TABLE IF NOT EXISTS {{DB}}.ver {{{OnCluster}}} (k UInt64, ver UInt64) ' +
          'ENGINE={{ReplacingMergeTree}}(ver) ORDER BY k', true)
    }
  }
  let ver = await _upgradeRequest(`SELECT max(ver) as ver FROM {{DB}}.ver WHERE k = ${key} FORMAT JSON`,
    true)
  ver = ver.data.data && ver.data.data[0] && ver.data.data[0].ver ? ver.data.data[0].ver : 0
  for (let i = parseInt(ver); i < scripts.length; ++i) {
    if (!scripts[i]) { continue }
    scripts[i] = scripts[i].trim()
    await _upgradeRequest(scripts[i], true, { key: key, to: i + 1 })
  }
}

/**
 * @param db {string} database to update
 * @param request {string} request tpl
 * @param useDefaultDB {boolean} use db as default db
 * @param updateVer {{key: number, to: number}} update ver table
 * @returns {Promise<AxiosResponse<any>>}
 */
const upgradeRequest = async (db, request, useDefaultDB, updateVer) => {
  const tpl = hb.compile(request)
  request = tpl({
    ...getEnv(),
    DB: db,
    CLUSTER: clusterName || '',
    SAMPLES_ORDER_RUL: samplesOrderingRule(),
    OnCluster: clusterName ? `ON CLUSTER \`${clusterName}\`` : '',
    MergeTree: clusterName ? 'ReplicatedMergeTree' : 'MergeTree',
    ReplacingMergeTree: clusterName ? 'ReplicatedReplacingMergeTree' : 'ReplacingMergeTree',
    AggregatingMergeTree: clusterName ? 'ReplicatedAggregatingMergeTree' : 'AggregatingMergeTree'
  })
  console.log(request)
  const res = await client.rawRequest(request, null, useDefaultDB ? db : undefined)
  if (updateVer) {
    await client.rawRequest(`INSERT INTO ${db}.ver (k, ver) VALUES (${updateVer.key}, ${updateVer.to})`, null, db)
  }
  return res
}

/**
 * @param opts {{db: string, samples_days: number, time_series_days: number, storage_policy: string}[]}
 * @returns {Promise<void>}
 */
module.exports.rotate = async (opts) => {
  for (const db of opts) {
    const settings = await client.getSettings([
      { type: 'rotate', name: 'v3_samples_days' },
      { type: 'rotate', name: 'v3_time_series_days' },
      { type: 'rotate', name: 'v3_storage_policy' },
      { type: 'rotate', name: 'v1_traces_days' },
      { type: 'rotate', name: 'v1_traces_storage_policy' }
    ], db.db)
    const _update = (req) => {
      return upgradeRequest(db.db, req, true)
    }
    if (db.samples_days + '' !== settings.v3_samples_days) {
      const alterTable = 'ALTER TABLE {{DB}}.samples_v3 {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE {{DB}}.samples_v3 {{{OnCluster}}} MODIFY TTL toDateTime(timestamp_ns / 1000000000) + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      await client.addSetting('rotate', 'v3_samples_days', db.samples_days + '', db.db)
    }
    if (db.time_series_days + '' !== settings.v3_time_series_days) {
      const alterTable = 'ALTER TABLE {{DB}}.time_series {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE {{DB}}.time_series {{{OnCluster}}} MODIFY TTL "date" + INTERVAL ${db.time_series_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      const alterView = 'ALTER TABLE {{DB}}.time_series_gin {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateView = `ALTER TABLE {{DB}}.time_series_gin {{{OnCluster}}} MODIFY TTL "date" + INTERVAL ${db.time_series_days} DAY`
      await _update(alterView, null, db.db)
      await _update(rotateView, null, db.db)
      await client.addSetting('rotate', 'v3_time_series_days', db.time_series_days + '', db.db)
    }
    if (db.storage_policy && db.storage_policy !== settings.v3_storage_policy) {
      logger.debug(`Altering storage policy: ${db.storage_policy}`)
      const alterTs = `ALTER TABLE {{DB}}.time_series {{{OnCluster}}} MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterTsVw = `ALTER TABLE {{DB}}.time_series_gin {{{OnCluster}}} MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterSm = `ALTER TABLE {{DB}}.samples_v3 {{{OnCluster}}} MODIFY SETTING storage_policy='${db.storage_policy}'`
      await _update(alterTs, null, db.db)
      await _update(alterTsVw, null, db.db)
      await _update(alterSm, null, db.db)
      await client.addSetting('rotate', 'v3_storage_policy', db.storage_policy, db.db)
    }
    if (db.samples_days + '' !== settings.v1_traces_days) {
      let alterTable = 'ALTER TABLE {{DB}}.tempo_traces {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      let rotateTable = `ALTER TABLE {{DB}}.tempo_traces {{{OnCluster}}} MODIFY TTL toDateTime(timestamp_ns / 1000000000) + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      alterTable = 'ALTER TABLE {{DB}}.tempo_traces_attrs_gin {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      rotateTable = `ALTER TABLE {{DB}}.tempo_traces_attrs_gin {{{OnCluster}}} MODIFY TTL date + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      alterTable = 'ALTER TABLE {{DB}}.tempo_traces_kv {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      rotateTable = `ALTER TABLE {{DB}}.tempo_traces_kv {{{OnCluster}}} MODIFY TTL date + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      await client.addSetting('rotate', 'v1_traces_days', db.samples_days + '', db.db)
    }
    if (db.storage_policy && db.storage_policy !== settings.v1_traces_storage_policy) {
      logger.debug(`Altering storage policy: ${db.storage_policy}`)
      const alterTs = `ALTER TABLE {{DB}}.tempo_traces MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterTsVw = `ALTER TABLE {{DB}}.tempo_traces_attrs_gin MODIFY SETTING storage_policy='${db.storage_policy}'`
      const alterSm = `ALTER TABLE {{DB}}.tempo_traces_kv MODIFY SETTING storage_policy='${db.storage_policy}'`
      await _update(alterTs, null, db.db)
      await _update(alterTsVw, null, db.db)
      await _update(alterSm, null, db.db)
      await client.addSetting('rotate', 'v1_traces_storage_policy', db.storage_policy, db.db)
    }
  }
}
