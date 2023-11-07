module.exports.overall = [
  `CREATE TABLE IF NOT EXISTS {{DB}}.time_series {{{OnCluster}}} (date Date,fingerprint UInt64,labels String, name String)
    ENGINE = {{ReplacingMergeTree}}(date) PARTITION BY date ORDER BY fingerprint`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.samples_v3 {{{OnCluster}}}
    (
      fingerprint UInt64,
      timestamp_ns Int64 CODEC(DoubleDelta),
      value Float64 CODEC(Gorilla),
      string String
    ) ENGINE = {{MergeTree}}
    PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
    ORDER BY ({{SAMPLES_ORDER_RUL}})`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.settings {{{OnCluster}}}
    (fingerprint UInt64, type String, name String, value String, inserted_at DateTime64(9, 'UTC'))
    ENGINE = {{ReplacingMergeTree}}(inserted_at) ORDER BY fingerprint`,

  'DROP TABLE IF EXISTS {{DB}}.samples_read {{{OnCluster}}}',

  `CREATE TABLE IF NOT EXISTS {{DB}}.samples_read {{{OnCluster}}}
   (fingerprint UInt64,timestamp_ms Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples|samples_v2)$')`,

  `CREATE VIEW IF NOT EXISTS {{DB}}.samples_read_v2_1 {{{OnCluster}}} AS 
    SELECT fingerprint, timestamp_ms * 1000000 as timestamp_ns, value, string FROM samples_read`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.samples_read_v2_2 {{{OnCluster}}}
   (fingerprint UInt64,timestamp_ns Int64,value Float64,string String)
   ENGINE=Merge('{{DB}}', '^(samples_read_v2_1|samples_v3)$')`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.time_series_gin {{{OnCluster}}} (
    date Date,
    key String,
    val String,
    fingerprint UInt64
   ) ENGINE = {{ReplacingMergeTree}}() PARTITION BY date ORDER BY (key, val, fingerprint)`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.time_series_gin_view {{{OnCluster}}} TO time_series_gin
   AS SELECT date, pairs.1 as key, pairs.2 as val, fingerprint
   FROM time_series ARRAY JOIN JSONExtractKeysAndValues(time_series.labels, 'String') as pairs`,

  "INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_5'), 'update', " +
     "'v3_1', toString(toUnixTimestamp(NOW())), NOW())",

  `CREATE TABLE IF NOT EXISTS {{DB}}.metrics_15s {{{OnCluster}}} (
      fingerprint UInt64,
      timestamp_ns Int64 CODEC(DoubleDelta),
      last AggregateFunction(argMax, Float64, Int64),
      max SimpleAggregateFunction(max, Float64),
      min SimpleAggregateFunction(min, Float64),
      count AggregateFunction(count),
      sum SimpleAggregateFunction(sum, Float64),
      bytes SimpleAggregateFunction(sum, Float64)
) ENGINE = {{AggregatingMergeTree}}
PARTITION BY toDate(toDateTime(intDiv(timestamp_ns, 1000000000)))
ORDER BY (fingerprint, timestamp_ns);`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.metrics_15s_mv {{{OnCluster}}} TO metrics_15s AS
SELECT fingerprint,
  intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
  argMaxState(value, samples.timestamp_ns) as last,
  maxSimpleState(value) as max,
  minSimpleState(value) as min,
  countState() as count,
  sumSimpleState(value) as sum,
  sumSimpleState(length(string)) as bytes
FROM samples_v3 as samples
GROUP BY fingerprint, timestamp_ns;`,

  "INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_2'), 'update', " +
     "'v3_2', toString(toUnixTimestamp(NOW())), NOW())"
]

module.exports.traces = [
  `CREATE TABLE IF NOT EXISTS {{DB}}.tempo_traces {{{OnCluster}}} (
    oid String DEFAULT '0',
    trace_id FixedString(16),
    span_id FixedString(8),
    parent_id String,
    name String,
    timestamp_ns Int64 CODEC(DoubleDelta),
    duration_ns Int64,
    service_name String,
    payload_type Int8,
    payload String
  ) Engine = {{MergeTree}}() ORDER BY (oid, trace_id, timestamp_ns)
  PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000))));`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.tempo_traces_attrs_gin {{{OnCluster}}} (
    oid String,
    date Date,
    key String,
    val String,
    trace_id FixedString(16),
    span_id FixedString(8),
    timestamp_ns Int64,
    duration Int64
  ) Engine = {{ReplacingMergeTree}}()
  PARTITION BY date
  ORDER BY (oid, date, key, val, timestamp_ns, trace_id, span_id);`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.tempo_traces_kv {{{OnCluster}}} (
    oid String,
    date Date,
    key String,
    val_id UInt64,
    val String
  ) Engine = {{ReplacingMergeTree}}()
  PARTITION BY (oid, date)
  ORDER BY (oid, date, key, val_id)`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.tempo_traces_kv_mv {{{OnCluster}}} TO tempo_traces_kv AS 
    SELECT oid, date, key, cityHash64(val) % 10000 as val_id, val FROM tempo_traces_attrs_gin`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.traces_input {{{OnCluster}}} (
    oid String DEFAULT '0',
    trace_id String,
    span_id String,
    parent_id String,
    name String,
    timestamp_ns Int64 CODEC(DoubleDelta),
    duration_ns Int64,
    service_name String,
    payload_type Int8,
    payload String,
    tags Array(Tuple(String, String))
   ) Engine=Null`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.traces_input_traces_mv {{{OnCluster}}} TO tempo_traces AS
    SELECT  oid, 
      unhex(trace_id)::FixedString(16) as trace_id,
      unhex(span_id)::FixedString(8) as span_id,
      unhex(parent_id) as parent_id,
      name,
      timestamp_ns,
      duration_ns,
      service_name,
      payload_type,
      payload
    FROM traces_input`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.traces_input_tags_mv {{{OnCluster}}} TO tempo_traces_attrs_gin AS
    SELECT  oid,
      toDate(intDiv(timestamp_ns, 1000000000)) as date,
      tags.1 as key, 
      tags.2 as val,
      unhex(trace_id)::FixedString(16) as trace_id, 
      unhex(span_id)::FixedString(8) as span_id, 
      timestamp_ns,      
      duration_ns as duration
    FROM traces_input ARRAY JOIN tags`,

  "INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('tempo_traces_v1'), 'update', " +
     "'tempo_traces_v2', toString(toUnixTimestamp(NOW())), NOW())"
]

module.exports.overall_dist = [
  `CREATE TABLE {{DB}}.metrics_15s_dist {{{OnCluster}}} (
    \`fingerprint\` UInt64,
    \`timestamp_ns\` Int64 CODEC(DoubleDelta),
    \`last\` AggregateFunction(argMax, Float64, Int64),
    \`max\` SimpleAggregateFunction(max, Float64),
    \`min\` SimpleAggregateFunction(min, Float64),
    \`count\` AggregateFunction(count),
    \`sum\` SimpleAggregateFunction(sum, Float64),
    \`bytes\` SimpleAggregateFunction(sum, Float64)
) ENGINE = Distributed('{{CLUSTER}}', '{{DB}}', 'metrics_15s', fingerprint)`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.samples_v3_dist {{{OnCluster}}} (
  \`fingerprint\` UInt64,
  \`timestamp_ns\` Int64 CODEC(DoubleDelta),
  \`value\` Float64 CODEC(Gorilla),
  \`string\` String
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'samples_v3', fingerprint)`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.time_series_dist {{{OnCluster}}} (
  \`date\` Date,
  \`fingerprint\` UInt64,
  \`labels\` String,
  \`name\` String
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'time_series', fingerprint);`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.settings_dist {{{OnCluster}}} (
  \`fingerprint\` UInt64,
  \`type\` String,
  \`name\` String,
  \`value\` String,
  \`inserted_at\` DateTime64(9, 'UTC')
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'settings', rand());`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.time_series_gin_dist {{{OnCluster}}} (
    date Date,
    key String,
    val String,
    fingerprint UInt64
   )  ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'time_series_gin', rand());`,
]

module.exports.traces_dist = [
  `CREATE TABLE IF NOT EXISTS tempo_traces_kv_dist {{{OnCluster}}} (
  oid String,
  date Date,
  key String,
  val_id String,
  val String
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'tempo_traces_kv', sipHash64(oid, key));`,

  `CREATE TABLE IF NOT EXISTS tempo_traces_dist {{{OnCluster}}} (
  oid String,
  trace_id FixedString(16),
  span_id FixedString(8),
  parent_id String,
  name String,
  timestamp_ns Int64 CODEC(DoubleDelta),
  duration_ns Int64,
  service_name String,
  payload_type Int8,
  payload String,
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'tempo_traces', sipHash64(oid, trace_id));`,

  `CREATE TABLE IF NOT EXISTS tempo_traces_attrs_gin_dist {{{OnCluster}}} (
  oid String,
  date Date,
  key String,
  val String,
  trace_id FixedString(16),
  span_id FixedString(8),
  timestamp_ns Int64,
  duration Int64
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'tempo_traces_attrs_gin', sipHash64(oid, trace_id));`
]
