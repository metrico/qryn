module.exports.overall = [
  `CREATE TABLE IF NOT EXISTS {{DB}}.time_series {{{OnCluster}}} (date Date,fingerprint UInt64,labels String, name String)
    ENGINE = {{ReplacingMergeTree}}(date) PARTITION BY date ORDER BY fingerprint {{{CREATE_SETTINGS}}}`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.samples_v3 {{{OnCluster}}}
    (
      fingerprint UInt64,
      timestamp_ns Int64 CODEC(DoubleDelta),
      value Float64 CODEC(Gorilla),
      string String
    ) ENGINE = {{MergeTree}}
    PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
    ORDER BY ({{SAMPLES_ORDER_RUL}}) {{{CREATE_SETTINGS}}}`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.settings {{{OnCluster}}}
    (fingerprint UInt64, type String, name String, value String, inserted_at DateTime64(9, 'UTC'))
    ENGINE = {{ReplacingMergeTree}}(inserted_at) ORDER BY fingerprint {{{CREATE_SETTINGS}}}`,

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
   ) ENGINE = {{ReplacingMergeTree}}() PARTITION BY date ORDER BY (key, val, fingerprint) {{{CREATE_SETTINGS}}}`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.time_series_gin_view {{{OnCluster}}} TO time_series_gin
   AS SELECT date, pairs.1 as key, pairs.2 as val, fingerprint
   FROM time_series ARRAY JOIN JSONExtractKeysAndValues(time_series.labels, 'String') as pairs`,

  `INSERT INTO {{DB}}.settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_5'), 'update',
     'v3_1', toString(toUnixTimestamp(NOW())), NOW())`,

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
ORDER BY (fingerprint, timestamp_ns) {{{CREATE_SETTINGS}}};`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.metrics_15s_mv {{{OnCluster}}} TO metrics_15s AS
SELECT fingerprint,
  intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
  argMaxState(value, samples.timestamp_ns) as last,
  maxSimpleState(value) as max,
  minSimpleState(value) as min,
  countState() as count,
  sumSimpleState(value) as sum,
  sumSimpleState(length(string)) as bytes
FROM {{DB}}.samples_v3 as samples
GROUP BY fingerprint, timestamp_ns;`,

  `INSERT INTO {{DB}}.settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_2'), 'update',
     'v3_2', toString(toUnixTimestamp(NOW())), NOW())`,
  "INSERT INTO {{DB}}.settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_2'), 'update', " +
     "'v3_2', toString(toUnixTimestamp(NOW())), NOW())",

  `ALTER TABLE {{DB}}.time_series {{{OnCluster}}} 
    ADD COLUMN IF NOT EXISTS type UInt8,
    MODIFY ORDER BY (fingerprint, type)`,

  `ALTER TABLE {{DB}}.samples_v3 {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS type UInt8`,

  `ALTER TABLE {{DB}}.time_series_gin {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS type UInt8,
    MODIFY ORDER BY (key, val, fingerprint, type)`,

  `ALTER TABLE {{DB}}.metrics_15s {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS type UInt8,
    MODIFY ORDER BY (fingerprint, timestamp_ns, type)`,

  'RENAME TABLE {{DB}}.time_series_gin_view TO time_series_gin_view_bak {{{OnCluster}}}',

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.time_series_gin_view {{{OnCluster}}} TO time_series_gin
   AS SELECT date, pairs.1 as key, pairs.2 as val, fingerprint, type
   FROM time_series ARRAY JOIN JSONExtractKeysAndValues(time_series.labels, 'String') as pairs`,

  'DROP TABLE IF EXISTS {{DB}}.time_series_gin_view_bak {{{OnCluster}}}',

  'RENAME TABLE {{DB}}.metrics_15s_mv TO metrics_15s_mv_bak {{{OnCluster}}}',

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.metrics_15s_mv {{{OnCluster}}} TO metrics_15s AS
SELECT fingerprint,
  intDiv(samples.timestamp_ns, 15000000000) * 15000000000 as timestamp_ns,
  argMaxState(value, samples.timestamp_ns) as last,
  maxSimpleState(value) as max,
  minSimpleState(value) as min,
  countState() as count,
  sumSimpleState(value) as sum,
  sumSimpleState(length(string)) as bytes,
  type
FROM samples_v3 as samples
GROUP BY fingerprint, timestamp_ns, type;`,

  'DROP TABLE IF EXISTS {{DB}}.metrics_15s_mv_bak {{{OnCluster}}}'
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
  PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000)))) {{{CREATE_SETTINGS}}};`,

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
  ORDER BY (oid, date, key, val, timestamp_ns, trace_id, span_id) {{{CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.tempo_traces_kv {{{OnCluster}}} (
    oid String,
    date Date,
    key String,
    val_id UInt64,
    val String
  ) Engine = {{ReplacingMergeTree}}()
  PARTITION BY (oid, date)
  ORDER BY (oid, date, key, val_id) {{{CREATE_SETTINGS}}}`,

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

  `INSERT INTO {{DB}}.settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('tempo_traces_v1'), 'update',
     'tempo_traces_v2', toString(toUnixTimestamp(NOW())), NOW())`
]

module.exports.overall_dist = [
  `CREATE TABLE IF NOT EXISTS {{DB}}.metrics_15s_dist {{{OnCluster}}} (
    \`fingerprint\` UInt64,
    \`timestamp_ns\` Int64 CODEC(DoubleDelta),
    \`last\` AggregateFunction(argMax, Float64, Int64),
    \`max\` SimpleAggregateFunction(max, Float64),
    \`min\` SimpleAggregateFunction(min, Float64),
    \`count\` AggregateFunction(count),
    \`sum\` SimpleAggregateFunction(sum, Float64),
    \`bytes\` SimpleAggregateFunction(sum, Float64)
) ENGINE = Distributed('{{CLUSTER}}', '{{DB}}', 'metrics_15s', fingerprint) {{{DIST_CREATE_SETTINGS}}};`,

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
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'time_series', fingerprint) {{{DIST_CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.settings_dist {{{OnCluster}}} (
  \`fingerprint\` UInt64,
  \`type\` String,
  \`name\` String,
  \`value\` String,
  \`inserted_at\` DateTime64(9, 'UTC')
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'settings', rand()) {{{DIST_CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.time_series_gin_dist {{{OnCluster}}} (
    date Date,
    key String,
    val String,
    fingerprint UInt64
   )  ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'time_series_gin', rand()) {{{DIST_CREATE_SETTINGS}}};`,

  'ALTER TABLE {{DB}}.metrics_15s_dist {{{OnCluster}}} ADD COLUMN IF NOT EXISTS `type` UInt8;',

  'ALTER TABLE {{DB}}.samples_v3_dist {{{OnCluster}}} ADD COLUMN IF NOT EXISTS `type` UInt8',

  'ALTER TABLE {{DB}}.time_series_dist {{{OnCluster}}} ADD COLUMN IF NOT EXISTS `type` UInt8;',

  'ALTER TABLE {{DB}}.time_series_gin_dist {{{OnCluster}}} ADD COLUMN IF NOT EXISTS `type` UInt8;'
]

module.exports.traces_dist = [
  `CREATE TABLE IF NOT EXISTS {{DB}}.tempo_traces_kv_dist {{{OnCluster}}} (
  oid String,
  date Date,
  key String,
  val_id String,
  val String
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'tempo_traces_kv', sipHash64(oid, key)) {{{DIST_CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.tempo_traces_dist {{{OnCluster}}} (
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
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'tempo_traces', sipHash64(oid, trace_id)) {{{DIST_CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.tempo_traces_attrs_gin_dist {{{OnCluster}}} (
  oid String,
  date Date,
  key String,
  val String,
  trace_id FixedString(16),
  span_id FixedString(8),
  timestamp_ns Int64,
  duration Int64
) ENGINE = Distributed('{{CLUSTER}}','{{DB}}', 'tempo_traces_attrs_gin', sipHash64(oid, trace_id)) {{{DIST_CREATE_SETTINGS}}};`
]

module.exports.profiles = [
  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_input {{{OnCluster}}} (
    timestamp_ns UInt64,
    type LowCardinality(String),
    service_name LowCardinality(String),
    sample_types_units Array(Tuple(String, String)),
    period_type LowCardinality(String),
    period_unit LowCardinality(String),
    tags Array(Tuple(String, String)),
    duration_ns UInt64,
    payload_type LowCardinality(String),
    payload String,
    values_agg Array(Tuple(String, Int64, Int32)) CODEC(ZSTD(1))
  ) Engine=Null`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles {{{OnCluster}}} (
    timestamp_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    sample_types_units Array(Tuple(String, String)) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    duration_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    payload_type LowCardinality(String) CODEC(ZSTD(1)),
    payload String CODEC(ZSTD(1)),
    values_agg Array(Tuple(String, Int64, Int32)) CODEC(ZSTD(1)) 
  ) Engine {{MergeTree}}() 
  ORDER BY (type_id, service_name, timestamp_ns)
  PARTITION BY toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000))) {{{CREATE_SETTINGS}}}`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.profiles_mv {{{OnCluster}}} TO profiles AS
    SELECT 
      timestamp_ns, 
      cityHash64(arraySort(arrayConcat(
        profiles_input.tags, [
          ('__type__', concatWithSeparator(':', type, period_type, period_unit) as _type_id), 
          ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
          ('service_name', service_name)
      ])) as _tags) as fingerprint,
      _type_id as type_id,
      sample_types_units,
      service_name,
      duration_ns,
      payload_type,
      payload,
      values_agg
    FROM profiles_input`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_series {{{OnCluster}}} (
    date Date CODEC(ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    sample_types_units Array(Tuple(String, String)) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),    
    tags Array(Tuple(String, String)) CODEC(ZSTD(1)),
  ) Engine {{ReplacingMergeTree}}() 
  ORDER BY (date, type_id, fingerprint)
  PARTITION BY date {{{CREATE_SETTINGS}}}`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.profiles_series_mv {{{OnCluster}}} TO profiles_series AS
    SELECT 
      toDate(intDiv(timestamp_ns, 1000000000)) as date,
      concatWithSeparator(':', type, period_type, period_unit) as type_id,
      sample_types_units,
      service_name,
      cityHash64(arraySort(arrayConcat(
        profiles_input.tags, [
          ('__type__', type_id), 
          ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
          ('service_name', service_name)
      ])) as _tags) as fingerprint,
      arrayConcat(profiles_input.tags, [('service_name', service_name)]) as tags
    FROM profiles_input`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_series_gin {{{OnCluster}}} (
    date Date CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1)),
    val String CODEC(ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    sample_types_units Array(Tuple(String, String)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
  ) Engine {{ReplacingMergeTree}}()
  ORDER BY (date, key, val, type_id, fingerprint)
  PARTITION BY date {{{CREATE_SETTINGS}}}`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.profiles_series_gin_mv {{{OnCluster}}} TO profiles_series_gin AS
    SELECT 
      date,
      kv.1 as key,
      kv.2 as val,
      type_id,
      sample_types_units,
      service_name,
      fingerprint
    FROM profiles_series ARRAY JOIN tags as kv`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_series_keys {{{OnCluster}}} (
    date Date,
    key String,
    val String,
    val_id UInt64
  ) Engine {{ReplacingMergeTree}}()
  ORDER BY (date, key, val_id)
  PARTITION BY date {{{CREATE_SETTINGS}}}`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.profiles_series_keys_mv {{{OnCluster}}} TO profiles_series_keys AS
    SELECT 
      date,
      key,
      val,
      cityHash64(val) % 50000 as val_id
    FROM profiles_series_gin`,

  `ALTER TABLE {{DB}}.profiles_input {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS \`tree\` Array(Tuple(UInt64, UInt64, UInt64, Array(Tuple(String, Int64, Int64)))),
    ADD COLUMN IF NOT EXISTS \`functions\` Array(Tuple(UInt64, String))`,

  `ALTER TABLE {{DB}}.profiles {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS \`tree\` Array(Tuple(UInt64, UInt64, UInt64, Array(Tuple(String, Int64, Int64)))),
    ADD COLUMN IF NOT EXISTS \`functions\` Array(Tuple(UInt64, String))`,

  'RENAME TABLE IF EXISTS {{DB}}.profiles_mv TO profiles_mv_bak {{{OnCluster}}}',

  `CREATE MATERIALIZED VIEW IF NOT EXISTS {{DB}}.profiles_mv {{{OnCluster}}} TO profiles AS
    SELECT 
      timestamp_ns, 
      cityHash64(arraySort(arrayConcat(
        profiles_input.tags, [
          ('__type__', concatWithSeparator(':', type, period_type, period_unit) as _type_id), 
          ('__sample_types_units__', arrayStringConcat(arrayMap(x -> x.1 || ':' || x.2, arraySort(sample_types_units)), ';')),
          ('service_name', service_name)
      ])) as _tags) as fingerprint,
      _type_id as type_id,
      sample_types_units,
      service_name,
      duration_ns,
      payload_type,
      payload,
      values_agg,
      tree,
      functions
    FROM profiles_input`,

  'DROP TABLE IF EXISTS {{DB}}.profiles_mv_bak {{{OnCluster}}}',

  "INSERT INTO {{DB}}.settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('profiles_v2'), 'update', " +
    "'profiles_v2', toString(toUnixTimestamp(NOW())), NOW())"
]

module.exports.profiles_dist = [
  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_dist {{{OnCluster}}} (
    timestamp_ns UInt64,
    fingerprint UInt64,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    duration_ns UInt64,
    payload_type LowCardinality(String),
    payload String,
    values_agg Array(Tuple(String, Int64, Int32))
  ) ENGINE = Distributed('{{CLUSTER}}','{{DB}}','profiles', fingerprint) {{{DIST_CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_series_dist {{{OnCluster}}} (
    date Date,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
    tags Array(Tuple(String, String)) CODEC(ZSTD(1))
  ) ENGINE = Distributed('{{CLUSTER}}','{{DB}}','profiles_series',fingerprint) {{{DIST_CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_series_gin_dist {{{OnCluster}}} (
    date Date,
    key String,
    val String,
    type_id LowCardinality(String),
    service_name LowCardinality(String),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1))
  ) ENGINE = Distributed('{{CLUSTER}}','{{DB}}','profiles_series_gin',fingerprint) {{{DIST_CREATE_SETTINGS}}};`,

  `CREATE TABLE IF NOT EXISTS {{DB}}.profiles_series_keys_dist {{{OnCluster}}} (
    date Date,
    key String,
    val String,
    val_id UInt64
  ) ENGINE = Distributed('{{CLUSTER}}','{{DB}}','profiles_series_keys', rand()) {{{DIST_CREATE_SETTINGS}}};`,

  `ALTER TABLE {{DB}}.profiles_dist {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS \`tree\` Array(Tuple(UInt64, UInt64, UInt64, Array(Tuple(String, Int64, Int64)))),
    ADD COLUMN IF NOT EXISTS \`functions\` Array(Tuple(UInt64, String))`,

  `ALTER TABLE {{DB}}.profiles_dist {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS \`sample_types_units\` Array(Tuple(String, String))`,

  `ALTER TABLE {{DB}}.profiles_series_dist {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS \`sample_types_units\` Array(Tuple(String, String))`,

  `ALTER TABLE {{DB}}.profiles_series_gin_dist {{{OnCluster}}}
    ADD COLUMN IF NOT EXISTS \`sample_types_units\` Array(Tuple(String, String))`
]
