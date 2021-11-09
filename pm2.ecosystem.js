module.exports = {
  apps: [{
    name: 'cloki',
    script: './cloki.js',
    env: {
      CLICKHOUSE_SERVER: 'localhost',
      CLICKHOUSE_PORT: 8123,
      CLICKHOUSE_AUTH: 'default:password',
      CLICKHOUSE_DB: 'cloki',
      TIMEFIELD: 'record_datetime',
      LABELS_DAYS: 7,
      SAMPLES_DAYS: 7,
      DEBUG: false
    }
  }]
}
