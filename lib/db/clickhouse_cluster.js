const UTILS = require('../utils')
const { samplesTableName, samplesReadTableName } = UTILS
const ClickHouse = require('@apla/clickhouse')
const axios = require('axios')

const rotationLabels = process.env.LABELS_DAYS || 7
const rotationSamples = process.env.SAMPLES_DAYS || 7
const storagePolicy = process.env.STORAGE_POLICY || false
const debug = process.env.DEBUG || false

/**
 *
 * @param host {string}
 * @param port {number}
 * @returns {{protocol: (string|string), readonly: boolean, port: (string|number),
 *  auth: (string|string), queryOptions: {database: (*|string)},
 *  host: (string|string)}|{protocol: (string|string), readonly: boolean, port: (number|number),
 *  auth: (string|string), queryOptions: {database: (*|string)}, host: (string|string)}}
 */
module.exports.clickhouseOptions = (host, port) => {
  if (!process.env.CLICKHOUSE_CLUSTERED) {
    return {
      host: process.env.CLICKHOUSE_SERVER || 'localhost',
      port: process.env.CLICKHOUSE_PORT || 8123,
      auth: process.env.CLICKHOUSE_AUTH || 'default:',
      protocol: process.env.CLICKHOUSE_PROTO ? process.env.CLICKHOUSE_PROTO + ':' : 'http:',
      readonly: !!process.env.READONLY,
      queryOptions: { database: process.env.CLICKHOUSE_DB || 'cloki' }
    }
  }
  return {
    host: host || (process.env.CLICKHOUSE_SERVER
      ? process.env.CLICKHOUSE_SERVER.split(';')[0].split(':')[0]
      : 'localhost'),
    port: port || (process.env.CLICKHOUSE_SERVER
      ? parseInt(process.env.CLICKHOUSE_SERVER.split(';')[0].split(':')[1])
      : 8123),
    auth: process.env.CLICKHOUSE_AUTH || 'default:',
    protocol: process.env.CLICKHOUSE_PROTO ? process.env.CLICKHOUSE_PROTO + ':' : 'http:',
    readonly: !!process.env.READONLY,
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'cloki' }
  }
}

const getAClient = () => new ClickHouse(module.exports.clickhouseOptions())
module.exports.getAClient = getAClient

const getAllOptions = () => {
  const urls = process.env.CLICKHOUSE_SERVER
    ? process.env.CLICKHOUSE_SERVER
    : 'localhost:8123'
  const chs = []
  for (const url of urls.split(';')) {
    const [host, port] = url.split(':')
    chs.push(module.exports.clickhouseOptions(host, parseInt(port)))
  }
  return chs
}

const query = (options, query) => {
  const clickhouseUrl = `${options.protocol}//${options.auth}@${options.host}:${options.port}`
  return axios.post(clickhouseUrl, query)
}

/**
 *
 * @param dbName {string}
 * @param _ch? {Object}
 * @param suffix? {string | undefined}
 * @returns {Promise<void>}
 */
module.exports.singleNodeInit = async (dbName, _ch, suffix) => {
  suffix = suffix || ''
  const ch = _ch || module.exports.clickhouseOptions()
  const dbQuery = 'CREATE DATABASE IF NOT EXISTS ' + dbName
  await query(ch, dbQuery)
  console.log('CREATE TABLES', dbName)

  let tsTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.time_series${suffix} (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint`
  let smTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.${samplesTableName}${suffix} (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (timestamp_ms)`
  const readTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.${samplesReadTableName}${suffix} (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE=Merge(\'${dbName}\', \'^(samples|samples_v[0-9]+${suffix})$\')`

  if (storagePolicy) {
    console.log('ADD SETTINGS storage policy', storagePolicy)
    const setStorage = ` SETTINGS storagePolicy='${storagePolicy}'`
    tsTable += setStorage
    smTable += setStorage
  }

  await query(ch, tsTable, undefined, function (err/*, data */) {
    if (err) { console.log(err); process.exit(1) } else if (debug) console.log('Timeseries Table ready!')
    return true
  })
  await query(ch, smTable, undefined, function (err/*, data */) {
    if (err) { console.log(err); process.exit(1) } else if (debug) console.log('Samples Table ready!')
    return true
  })
  await query(ch, readTable, undefined, function (err) {
    if (err) { console.log(err); process.exit(1) } else if (debug) console.log('Samples Table ready!')
    return true
  })

  if (rotationSamples > 0) {
    const alterTable = 'ALTER TABLE ' + dbName + `.${samplesTableName}${suffix} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192`
    const rotateTable = 'ALTER TABLE ' + dbName + `.${samplesTableName}${suffix} MODIFY TTL toDateTime(timestamp_ms / 1000)  + INTERVAL ` + rotationSamples + ' DAY'
    await query(ch, alterTable, undefined, function (err/*, data */) {
      if (err) { console.log(err) } else if (debug) console.log('Samples Table altered for rotation!')
      // return true;
    })
    await query(ch, rotateTable, undefined, function (err/*, data */) {
      if (err) { console.log(err) } else if (debug) console.log('Samples Table rotation set to days: ' + rotationSamples)
      return true
    })
  }

  if (rotationLabels > 0) {
    const alterTable = 'ALTER TABLE ' + dbName + `.time_series${suffix} MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192`
    const rotateTable = 'ALTER TABLE ' + dbName + `.time_series${suffix} MODIFY TTL date  + INTERVAL ` + rotationLabels + ' DAY'
    try {
      await query(ch, alterTable, undefined, function (err/*, data */) {
        if (err) { console.log(err) } else if (debug) console.log('Labels Table altered for rotation!')
        return true
      })
    } catch (e) {
      console.log(e.stack)
      e.request && console.log(e.request.data)
      e.response && console.log(e.response.data)
    }
    try {
      await query(ch, rotateTable, undefined, function (err/*, data */) {
        if (err) { console.log(err) } else if (debug) console.log('Labels Table rotation set to days: ' + rotationLabels)
        return true
      })
    } catch (e) {
      console.log(e.stack)
      e.request && console.log(e.request.data)
      e.response && console.log(e.response.data)
    }
  }

  if (storagePolicy) {
    console.log('ALTER storage policy', storagePolicy)
    const alterTs = `ALTER TABLE ${dbName}.time_series${suffix} MODIFY SETTING storagePolicy='${storagePolicy}'`
    const alterSm = `ALTER TABLE ${dbName}.${samplesTableName} MODIFY SETTING storagePolicy='${storagePolicy}'`

    await query(ch, alterTs, undefined, function (err/*, data */) {
      if (err) { console.log(err) } else if (debug) console.log('Storage policy update for fingerprints ' + storagePolicy)
      return true
    })
    await query(ch, alterSm, undefined, function (err/*, data */) {
      if (err) { console.log(err) } else if (debug) console.log('Storage policy update for samples ' + storagePolicy)
      return true
    })
  }
}

module.exports.clusterInit = async (dbName) => {
  const clusterName = process.env.CLICKHOUSE_CLUSTERED
  const tsTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.time_series as ${dbName}.time_series_standalone ENGINE = Distributed(${clusterName}, ${dbName}, time_series_standalone, fingerprint);`
  const readTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.${samplesReadTableName} as ${dbName}.${samplesReadTableName}_standalone ENGINE = Distributed(${clusterName}, ${dbName}, ${samplesReadTableName}_standalone, fingerprint);`
  const writeTable = 'CREATE TABLE IF NOT EXISTS ' + dbName + `.${samplesTableName} as ${dbName}.${samplesTableName}_standalone ENGINE = Distributed(${clusterName}, ${dbName}, ${samplesTableName}_standalone, fingerprint);`
  for (const ch of getAllOptions()) {
    console.log('CLIENT!!!')
    await module.exports.singleNodeInit(dbName, ch, '_standalone')
    await query(ch, tsTable, undefined, () => {})
    await query(ch, readTable, undefined, () => {})
    await query(ch, writeTable, undefined, () => {})
    console.log('CLIENT OK!!!')
  }
}

module.exports.init = (dbName) => process.env.CLICKHOUSE_CLUSTERED
  ? module.exports.clusterInit(dbName)
  : module.exports.singleNodeInit(dbName)
