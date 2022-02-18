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
   (fingerprint UInt64,timestamp_ns Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples|samples_v2)$')`,

  `CREATE VIEW IF NOT EXISTS samples_read_v2_1 AS 
    SELECT fingerprint, timestamp_ns * 1000000 as timestamp_ns, value, string FROM samples_read`,

  `CREATE TABLE IF NOT EXISTS samples_read_v2_2
   (fingerprint UInt64,timestamp_ns Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples_read_v2_1|samples_v3)$')`
]
