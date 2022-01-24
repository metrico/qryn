CREATE TABLE IF NOT EXISTS time_series (date Date,fingerprint UInt64,labels String, name String)
ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint;

CREATE TABLE IF NOT EXISTS samples_v2
(fingerprint UInt64,timestamp_ms Int64,value Float64,string String)
ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (timestamp_ms);

CREATE TABLE IF NOT EXISTS samples_read
(fingerprint UInt64,timestamp_ms Int64,value Float64,string String)
ENGINE=Merge('{{DB}}', '(samples|samples_v[0-9]+)');

CREATE TABLE IF NOT EXISTS settings
(fingerprint UInt64, type String, name String, value String, inserted_at DateTime64(9, 'UTC'))
ENGINE = ReplacingMergeTree(inserted_at) ORDER BY fingerprint;
