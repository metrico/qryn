module.exports.overall = [
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
    ORDER BY ({{SAMPLES_ORDER_RUL}})`,

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

  `INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_5'), 'update',
     'v3_1', toString(toUnixTimestamp(NOW())), NOW())`,

  `CREATE TABLE IF NOT EXISTS metrics_15s (
      fingerprint UInt64,
      timestamp_ns Int64 CODEC(DoubleDelta),
      last AggregateFunction(argMax, Float64, Int64),
      max SimpleAggregateFunction(max, Float64),
      min SimpleAggregateFunction(min, Float64),
      count AggregateFunction(count),
      sum SimpleAggregateFunction(sum, Float64),
      bytes SimpleAggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(toDateTime(intDiv(timestamp_ns, 1000000000)))
ORDER BY (fingerprint, timestamp_ns);`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_15s_mv TO metrics_15s AS
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

  `INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('update_v3_2'), 'update',
     'v3_2', toString(toUnixTimestamp(NOW())), NOW())`
]

module.exports.traces = [
  `CREATE TABLE IF NOT EXISTS tempo_traces (
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
  ) Engine MergeTree() ORDER BY (oid, trace_id, timestamp_ns)
  PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000))));`,

  `CREATE TABLE IF NOT EXISTS tempo_traces_attrs_gin (
    oid String,
    date Date,
    key String,
    val String,
    trace_id FixedString(16),
    span_id FixedString(8),
    timestamp_ns Int64,
    duration Int64
  ) Engine = ReplacingMergeTree()
  PARTITION BY date
  ORDER BY (oid, date, key, val, timestamp_ns, trace_id, span_id);`,

  `CREATE TABLE IF NOT EXISTS tempo_traces_kv (
    oid String,
    date Date,
    key String,
    val_id UInt64,
    val String
  ) Engine = ReplacingMergeTree()
  PARTITION BY (oid, date)
  ORDER BY (oid, date, key, val_id)`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS tempo_traces_kv_mv TO tempo_traces_kv AS 
    SELECT oid, date, key, cityHash64(val) % 10000 as val_id, val FROM tempo_traces_attrs_gin`,

  `CREATE TABLE IF NOT EXISTS traces_input (
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

  `CREATE MATERIALIZED VIEW IF NOT EXISTS traces_input_traces_mv TO tempo_traces AS
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

  `CREATE MATERIALIZED VIEW IF NOT EXISTS traces_input_tags_mv TO tempo_traces_attrs_gin AS
    SELECT  oid,
      toDate(intDiv(timestamp_ns, 1000000000)) as date,
      tags.1 as key, 
      tags.2 as val,
      unhex(trace_id)::FixedString(16) as trace_id, 
      unhex(span_id)::FixedString(8) as span_id, 
      timestamp_ns,      
      duration_ns as duration
    FROM traces_input ARRAY JOIN tags`,

  `INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('tempo_traces_v1'), 'update',
     'tempo_traces_v2', toString(toUnixTimestamp(NOW())), NOW())`
]

// timestamp profile id for better compression

// cant merge profiles in ch

// frame_stack Array(Tuple(UInt64, UInt64, UInt64, String, String, Bool, Bool, Bool, Bool, Array(Tuple()))),` + 
// // [{memory_start, memory_limit, file_offset, filename, build_id, has_filenames, has_functions, has_line_numbers, has_inline_frames, 
// // lines:[{line_num, func_name, func_sys_name, filename, func_start_line}], is_folded},...]
// // pprof stack array is top-down (though it doesnt matter because we need both x-axis and y-axis merges)
// `value UInt64,

// SelectMergeProfile query method is not necessary for grafana visualization, the schema should be backward compatible
// Diff query method is only necessary for diff view which is not available in oss grafana, the schema should be backward compatible
// SelectSeries metric query method is not necessary
module.exports.profiles = [
  `CREATE TABLE IF NOT EXISTS profiles_input (
    oid LowCardinality(String) DEFAULT '0' CODEC(ZSTD(1)),
    timestamp_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    type LowCardinality(String) CODEC(ZSTD(1)),` + // group by instead distinct to open for optimizations, profiletypes query: select type from profiles_input group by type; https://github.com/ClickHouse/ClickHouse/issues/4670
    `sample_type LowCardinality(String) CODEC(ZSTD(1)),
    sample_unit LowCardinality(String) CODEC(ZSTD(1)),
    period_type LowCardinality(String) CODEC(ZSTD(1)),
    period_unit LowCardinality(String) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    labels Array(Tuple(String, String)) CODEC(ZSTD(1)),
    profile_id String CODEC(ZSTD(1)),
    duration_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    payload_type LowCardinality(String) CODEC(ZSTD(1)),
    payload String CODEC(ZSTD(1))
  ) Engine=Null`,

  `CREATE TABLE IF NOT EXISTS pyroscope_profiles (
    oid LowCardinality(String) DEFAULT '0' CODEC(ZSTD(1)),
    timestamp_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    labels Array(Tuple(String, String)) CODEC(ZSTD(1)),
    profile_id String CODEC(ZSTD(1)),
    duration_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    payload_type LowCardinality(String) CODEC(ZSTD(1)),
    payload String CODEC(ZSTD(1))
  ) Engine MergeTree() 
  ORDER BY (oid, timestamp_ns, type_id, service_name)
  PARTITION BY (oid, toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000))))`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS profiles_input_mv TO pyroscope_profiles AS
    SELECT oid,
      timestamp_ns,
      arrayStringConcat([type, sample_type, sample_unit, period_type, period_unit], ':') as type_id,
      service_name,
      labels,
      unhex(profile_id)::FixedString(16) as profile_id,
      duration_ns,
      payload_type,
      payload
    FROM profiles_input`,

  `CREATE TABLE IF NOT EXISTS pyroscope_profiles_attrs_gin (
    oid LowCardinality(String) CODEC(ZSTD(1)),
    date Date CODEC(Delta, ZSTD(1)),
    key String CODEC(ZSTD(1)),
    val String CODEC(ZSTD(1)),
    profile_ids AggregateFunction(groupUniqArray, FixedString(16)) CODEC(ZSTD(1))
  ) Engine = AggregatingMergeTree()
  ORDER BY (oid, date, key, val)
  PARTITION BY (oid, date)`,

  `CREATE MATERIALIZED VIEW IF NOT EXISTS profiles_input_attrs_mv TO pyroscope_profiles_attrs_gin AS
    SELECT oid,
      toDate(intDiv(timestamp_ns, 1000000000)) as date,
      labels.1 as key, 
      labels.2 as val,
      groupUniqArrayState(profile_id)
    FROM profiles_input
    GROUP BY oid, date, key, val`,

  `INSERT INTO settings (fingerprint, type, name, value, inserted_at) VALUES (cityHash64('pyroscope_profiles_v1'), 'update',
    'pyroscope_profiles_v1', toString(toUnixTimestamp(NOW())), NOW())`
]
