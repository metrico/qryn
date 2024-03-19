const hb = require('handlebars')
const client = require('../clickhouse')
const logger = require('../../logger')
const { samplesOrderingRule, clusterName } = require('../../../common')
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
 * @param db {{name: string, storage_policy: string}}
 * @returns {Promise<void>}
 */
module.exports.upgrade = async (db) => {
  await upgradeSingle(db.name, 1, scripts.overall, db.storage_policy)
  await upgradeSingle(db.name, 2, scripts.traces, db.storage_policy)
  await upgradeSingle(db.name, 5, scripts.profiles, db.storage_policy)
  if (db.storage_policy) {
    await client.addSetting('rotate', 'v3_storage_policy', db.storage_policy, db.name)
    await client.addSetting('rotate', 'v1_traces_storage_policy', db.storage_policy, db.name)
  }
  if (clusterName) {
    await upgradeSingle(db.name, 3, scripts.overall_dist, db.storage_policy)
    await upgradeSingle(db.name, 4, scripts.traces_dist, db.storage_policy)
    await upgradeSingle(db.name, 6, scripts.profiles_dist, db.storage_policy)
  }
}

let isDBCreated = false
/**
 *
 * @param db {string}
 * @param key {number}
 * @param scripts {string[]}
 * @param storagePolicy {string}
 */
const upgradeSingle = async (db, key, scripts, storagePolicy) => {
  const _upgradeRequest = (request, useDefaultDB, updateVer) => {
    return upgradeRequest({ db, useDefaultDB, updateVer, storage_policy: storagePolicy }, request)
  }
  if (!isDBCreated) {
    isDBCreated = true
    await _upgradeRequest('CREATE DATABASE IF NOT EXISTS {{DB}} {{{OnCluster}}}')
    if (clusterName) {
      await _upgradeRequest('CREATE TABLE IF NOT EXISTS {{DB}}._ver {{{OnCluster}}} (k UInt64, ver UInt64) ' +
          'ENGINE={{ReplacingMergeTree}}(ver) ORDER BY k {{{CREATE_SETTINGS}}}', true)
      await _upgradeRequest('CREATE TABLE IF NOT EXISTS {{DB}}.ver {{{OnCluster}}} (k UInt64, ver UInt64) ' +
          'ENGINE=Distributed(\'{{CLUSTER}}\',\'{{DB}}\', \'_ver\', rand())', true)
    } else {
      await _upgradeRequest('CREATE TABLE IF NOT EXISTS {{DB}}.ver {{{OnCluster}}} (k UInt64, ver UInt64) ' +
          'ENGINE={{ReplacingMergeTree}}(ver) ORDER BY k {{{CREATE_SETTINGS}}}', true)
    }
  }
  let ver = await client.rawRequest(`SELECT max(ver) as ver FROM ver WHERE k = ${key} FORMAT JSON`,
    null, db)
  ver = ver.data.data && ver.data.data[0] && ver.data.data[0].ver ? ver.data.data[0].ver : 0
  for (let i = parseInt(ver); i < scripts.length; ++i) {
    if (!scripts[i]) { continue }
    scripts[i] = scripts[i].trim()
    await _upgradeRequest(scripts[i], true, { key: key, to: i + 1 })
  }
}

/**
 * @param opts {{db: string, useDefaultDB: boolean, updateVer: {key: number, to: number}, storage_policy: string}}
 * @param request {string} database to update
 * @returns {Promise<void>}
 */
const upgradeRequest = async (opts, request) => {
  const tpl = hb.compile(request)
  request = tpl({
    ...getEnv(),
    DB: opts.db,
    CLUSTER: clusterName || '',
    SAMPLES_ORDER_RUL: samplesOrderingRule(),
    OnCluster: clusterName ? `ON CLUSTER \`${clusterName}\`` : '',
    MergeTree: clusterName ? 'ReplicatedMergeTree' : 'MergeTree',
    ReplacingMergeTree: clusterName ? 'ReplicatedReplacingMergeTree' : 'ReplacingMergeTree',
    AggregatingMergeTree: clusterName ? 'ReplicatedAggregatingMergeTree' : 'AggregatingMergeTree',
    CREATE_SETTINGS: opts.storage_policy ? `SETTINGS storage_policy='${opts.storage_policy}'` : ''
  })
  await client.rawRequest(request, null, opts.useDefaultDB ? opts.db : undefined)
  if (opts.updateVer) {
    await client.rawRequest(`INSERT INTO ver (k, ver) VALUES (${opts.updateVer.key}, ${opts.updateVer.to})`,
      null, opts.db)
  }
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
      { type: 'rotate', name: 'v1_traces_storage_policy' },
      { type: 'rotate', name: 'v1_profiles_days' }
    ], db.db)
    const _update = (req) => {
      return upgradeRequest({ db: db.db, useDefaultDB: true }, req)
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
      const alterView = 'ALTER TABLE time_series_gin {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateView = `ALTER TABLE time_series_gin {{{OnCluster}}} MODIFY TTL "date" + INTERVAL ${db.time_series_days} DAY`
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
    if (db.samples_days + '' !== settings.v1_profiles_days) {
      let alterTable = 'ALTER TABLE {{DB}}.profiles {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      let rotateTable = `ALTER TABLE {{DB}}.profiles {{{OnCluster}}} MODIFY TTL toDateTime(timestamp_ns / 1000000000) + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      alterTable = 'ALTER TABLE {{DB}}.profiles_series {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      rotateTable = `ALTER TABLE {{DB}}.profiles_series {{{OnCluster}}} MODIFY TTL date + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      alterTable = 'ALTER TABLE {{DB}}.profiles_series_gin {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      rotateTable = `ALTER TABLE {{DB}}.profiles_series_gin {{{OnCluster}}} MODIFY TTL date + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      alterTable = 'ALTER TABLE {{DB}}.profiles_series_keys {{{OnCluster}}} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      rotateTable = `ALTER TABLE {{DB}}.profiles_series_keys {{{OnCluster}}} MODIFY TTL date + INTERVAL ${db.samples_days} DAY`
      await _update(alterTable, null, db.db)
      await _update(rotateTable, null, db.db)
      await client.addSetting('rotate', 'v1_profiles_days', db.samples_days + '', db.db)
    }
  }
}
