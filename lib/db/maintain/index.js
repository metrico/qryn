const hb = require('handlebars')
const client = require('../clickhouse')
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
 */
module.exports.upgrade = async (db) => {
  const _scripts = require('./scripts')
  await client.rawRequest(`CREATE DATABASE IF NOT EXISTS ${db}`)
  await client.rawRequest('CREATE TABLE IF NOT EXISTS ver (k UInt64, ver UInt64) ' +
    'ENGINE=ReplacingMergeTree(ver) ORDER BY k', null, db)
  let ver = await client.rawRequest('SELECT max(ver) as ver FROM ver FORMAT JSON', null, db)
  ver = ver.data.data && ver.data.data[0] && ver.data.data[0].ver ? ver.data.data[0].ver : 0
  const scripts = _scripts.slice(ver, _scripts.length)
  for (let i = parseInt(ver); i < scripts.length; ++i) {
    if (!scripts[i]) { continue }
    scripts[i] = scripts[i].trim()
    const tpl = hb.compile(scripts[i])
    scripts[i] = tpl({ ...getEnv(), DB: db })
    await client.rawRequest(scripts[i], null, db)
    await client.rawRequest(`INSERT INTO ver (k, ver) VALUES (1, ${i + 1})`, null, db)
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
      { type: 'rotate', name: 'v3_storage_policy' }
    ], db.db)
    if (db.samples_days + '' !== settings.v3_samples_days) {
      const alterTable = 'ALTER TABLE samples_v3 MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE samples_v3 MODIFY TTL toDateTime(timestamp_ns / 1000000000) + INTERVAL ${db.samples_days} DAY`
      await client.rawRequest(alterTable, null, db.db)
      await client.rawRequest(rotateTable, null, db.db)
      await client.addSetting('rotate', 'v3_samples_days', db.samples_days + '', db.db)
    }
    if (db.time_series_days + '' !== settings.v3_time_series_days) {
      const alterTable = 'ALTER TABLE time_series MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE time_series MODIFY TTL "date" + INTERVAL ${db.time_series_days} DAY`
      await client.rawRequest(alterTable, null, db.db)
      await client.rawRequest(rotateTable, null, db.db)
      await client.addSetting('rotate', 'v3_time_series_days', db.time_series_days + '', db.db)
    }
    if (db.storage_policy && db.storage_policy !== settings.v3_storage_policy) {
      console.log('ALTER storage policy', db.storage_policy)
      const alterTs = `ALTER TABLE time_series MODIFY SETTING storagePolicy='${db.storage_policy}'`
      const alterSm = `ALTER TABLE samples_v2 MODIFY SETTING storagePolicy='${db.storage_policy}'`
      await client.rawRequest(alterTs, null, db.db)
      await client.rawRequest(alterSm, null, db.db)
      await client.addSetting('rotate', 'v3_storage_policy', db.storage_policy, db.db)
    }
  }
}
