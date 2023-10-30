const UTILS = require('../utils')
const { samplesTableName, samplesReadTableName } = UTILS

const clickhouseOptions = {
  host: process.env.CLICKHOUSE_SERVER || 'localhost',
  port: process.env.CLICKHOUSE_PORT || 8123,
  auth: process.env.CLICKHOUSE_AUTH || 'default:',
  protocol: process.env.CLICKHOUSE_PROTO ? process.env.CLICKHOUSE_PROTO + ':' : 'http:',
  readonly: !!process.env.READONLY,
  queryOptions: { database: process.env.CLICKHOUSE_DB || 'cloki' }
}

function getClickhouseUrl () {
  return `${clickhouseOptions.protocol}//${clickhouseOptions.auth}@${clickhouseOptions.host}:${clickhouseOptions.port}`
}

module.exports = {
  samplesTableName,
  samplesReadTableName,
  getClickhouseUrl,
  databaseOptions: clickhouseOptions
}
