const fs = require('fs')
const path = require('path')
const hb = require('handlebars')
const CLokiClient = require('../clickhouse').client
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
 * @param clients {CLokiClient[]}
 */
module.exports.upgrade = async (clients) => {
  let scripts = fs.readFileSync(path.join(__dirname, 'scripts.sql'), { encoding: 'utf8' })
  const tpl = hb.compile(scripts)
  for (const client of clients) {
    const url = new URL(client.getClickhouseUrl().toString())
    const db = url.searchParams.get('database')
    url.searchParams.delete('database')
    const _client = new CLokiClient(url.toString())
    await _client.rawRequest(`CREATE DATABASE IF NOT EXISTS ${db}`)
    await client.rawRequest('CREATE TABLE IF NOT EXISTS ver (k UInt64, ver UInt64) ' +
      'ENGINE=ReplacingMergeTree(ver) ORDER BY k')
    let ver = await client.rawRequest('SELECT max(ver) as ver FROM ver FORMAT JSON')
    ver = ver.data.data && ver.data.data[0] && ver.data.data[0].ver ? ver.data.data[0].ver : 0
    scripts = tpl({ ...getEnv(), DB: db }).split(';\n')
    scripts.slice(ver, scripts.length)
    for (let i = parseInt(ver); i < scripts.length; ++i) {
      scripts[i] = scripts[i].trim()
      if (!scripts[i]) { continue }
      await client.rawRequest(scripts[i])
      await client.rawRequest(`INSERT INTO ver (k, ver) VALUES (1, ${i + 1})`)
    }
  }
}

/**
 * @param opts {{client: CLokiClient, samples_days: number, time_series_days: number, storage_policy: string}[]}
 * @returns {Promise<void>}
 */
module.exports.rotate = async (opts) => {
  for (const db of opts) {
    const settings = await db.client.getSettings([
      { type: 'rotate', name: 'samples_days' },
      { type: 'rotate', name: 'time_series_days' },
      { type: 'rotate', name: 'storage_policy' }
    ])
    if (db.samples_days + '' !== settings.samples_days) {
      const alterTable = 'ALTER TABLE samples_v2 MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE samples_v2 MODIFY TTL toDateTime(timestamp_ms / 1000) + INTERVAL ${db.samples_days} DAY`
      await db.client.rawRequest(alterTable)
      await db.client.rawRequest(rotateTable)
      await db.client.addSetting('rotate', 'samples_days', db.samples_days + '')
    }
    if (db.time_series_days + '' !== settings.time_series_days) {
      const alterTable = 'ALTER TABLE time_series MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192'
      const rotateTable = `ALTER TABLE time_series MODIFY TTL "date" + INTERVAL ${db.time_series_days} DAY`
      await db.client.rawRequest(alterTable)
      await db.client.rawRequest(rotateTable)
      await db.client.addSetting('rotate', 'time_series_days', db.time_series_days + '')
    }
    if (db.storage_policy && db.storage_policy !== settings.storage_policy) {
      console.log('ALTER storage policy', db.storage_policy)
      const alterTs = `ALTER TABLE time_series MODIFY SETTING storagePolicy='${db.storage_policy}'`
      const alterSm = `ALTER TABLE samples_v2 MODIFY SETTING storagePolicy='${db.storage_policy}'`
      await db.client.rawRequest(alterTs)
      await db.client.rawRequest(alterSm)
      await db.client.addSetting('rotate', 'storage_policy', db.storage_policy)
    }
  }
}
