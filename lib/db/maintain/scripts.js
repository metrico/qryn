module.exports = [
  `CREATE TABLE IF NOT EXISTS time_series (date Date,fingerprint UInt64,labels String, name String)
    ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint`,

  `CREATE TABLE IF NOT EXISTS samples_v3
    (
      fingerprint UInt64,
      timestamp_ns Int64 CODEC(DoubleDelta),
      value Float64 CODEC(Gorilla),
      string String
    ) ENGINE = MergeTree
    PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
    ORDER BY (timestamp_ns)`,

  `CREATE TABLE IF NOT EXISTS settings
    (fingerprint UInt64, type String, name String, value String, inserted_at DateTime64(9, 'UTC'))
    ENGINE = ReplacingMergeTree(inserted_at) ORDER BY fingerprint`,

  'DROP TABLE IF EXISTS samples_read',

  `CREATE TABLE IF NOT EXISTS samples_read
   (fingerprint UInt64,timestamp_ms Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples|samples_v2)$')`,

  `CREATE VIEW IF NOT EXISTS samples_read_v2_1 AS 
    SELECT fingerprint, timestamp_ms * 1000000 as timestamp_ns, value, string FROM samples_read`,

  `CREATE TABLE IF NOT EXISTS samples_read_v2_2
   (fingerprint UInt64,timestamp_ns Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples_read_v2_1|samples_v3)$')`,

  `CREATE TABLE IF NOT EXISTS time_series_gin (
    date Date,
    key String,
    val String,
    fingerprint UInt64
   ) ENGINE = ReplacingMergeTree() PARTITION BY date ORDER BY (key, val, fingerprint)`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS time_series_gin_view TO time_series_gin
   AS SELECT date, pairs.1 as key, pairs.2 as val, fingerprint
   FROM time_series ARRAY JOIN JSONExtractKeysAndValues(time_series.labels, 'String') as pairs`,

  `create table if not exists samples_v4 (
    fingerprint  UInt64 CODEC(DoubleDelta),
    labels Array(Tuple(String, String)),
    timestamp_ns Int64,
    value        Float64 CODEC(Gorilla),
    string       String
  ) Engine = MergeTree PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
  ORDER BY timestamp_ns
  SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600`,

  `CREATE VIEW IF NOT EXISTS samples_read_v4_1 AS 
   SELECT samples_read_v2_2.fingerprint as fingerprint, 
     JSONExtractKeysAndValues(time_series.labels, 'String') as labels, 
     timestamp_ns, value, string 
   FROM samples_read_v2_2 INNER ANY JOIN time_series ON time_series.fingerprint == samples_read_v2_2.fingerprint`,

  `CREATE TABLE IF NOT EXISTS samples_read_v4_2
   (fingerprint UInt64, labels Array(Tuple(String, String)),timestamp_ns Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples_read_v4_1|samples_v4)$')`
]
