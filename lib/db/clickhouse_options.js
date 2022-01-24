/**
 *
 * @returns {{protocol: (string|string), readonly: boolean, port: (*|number), auth: (*|string), db: string, host: string}}
 */
module.exports.options = () => ({
  host: process.env.CLICKHOUSE_SERVER || 'localhost',
  port: process.env.CLICKHOUSE_PORT || 8123,
  auth: process.env.CLICKHOUSE_AUTH || 'default:',
  protocol: process.env.CLICKHOUSE_PROTO ? process.env.CLICKHOUSE_PROTO + ':' : 'http:',
  readonly: !!process.env.READONLY,
  db: process.env.CLICKHOUSE_DB || 'cloki'
})

module.exports.getUrl = () => {
  const opts = module.exports.options()
  return `${opts.protocol}//${opts.auth}@${opts.host}:${opts.port}`
}
