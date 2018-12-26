# cLoki
###### like Loki, but for Clickhouse.

Super experimental [Loki](https://github.com/grafana/loki) emulator based on [PromHouse](https://github.com/Percona-Lab/PromHouse) schema

Loki API Functions are loosely implemented as documented by the [Loki API](https://github.com/grafana/loki/blob/master/docs/api.md) reference.

* Do NOT try this, ever.

Status
* [x] Hash Fingerprinting (NOT compatible with Prometheus)
* [ ] Push API
* [ ] Query API
* [ ] Labels API

### Database Schema
```
CREATE TABLE time_series (
    date Date,
    fingerprint UInt64,
    labels String
)
ENGINE = ReplacingMergeTree
    PARTITION BY date
    ORDER BY fingerprint;

CREATE TABLE samples (
    fingerprint UInt64,
    timestamp_ms Int64,
    value Float64,
    string String,
)
ENGINE = MergeTree
    PARTITION BY toDate(timestamp_ms / 1000)
    ORDER BY (fingerprint, timestamp_ms);
```

### Usage Examples
```
SELECT * FROM time_series WHERE fingerprint = 7975981685167825999;
```
```
┌date┬fingerprint┬labels┐
│ 2017-12-31 │ 7975981685167825999 │ {"__name__":"up","instance":"promhouse_clickhouse_exporter_1:9116","job":"clickhouse"} │
└┴┴┘
```
```
SELECT * FROM samples WHERE fingerprint = 7975981685167825999 LIMIT 3;
```
```
┌fingerprint┬timestamp_ms┬value┐
│ 7975981685167825999 │ 1514730532900 │     0 │
│ 7975981685167825999 │ 1514730533901 │     1 │
│ 7975981685167825999 │ 1514730534901 │     1 │
└┴┴┘
```

### Raw Queries

#### CREATE
##### DATABASE
```
CREATE DATABASE IF NOT EXISTS loki
```
##### TABLES
```
CREATE TABLE IF NOT EXISTS loki.time_series (date Date,fingerprint UInt64,labels String) ENGINE = ReplacingMergeTree PARTITION BY date ORDER BY fingerprint;
CREATE TABLE IF NOT EXISTS loki.samples (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toDate(timestamp_ms / 1000) ORDER BY (fingerprint, timestamp_ms);
```

#### SELECT
##### FINGERPRINTS
```
SELECT DISTINCT fingerprint, labels FROM loki.time_series
```
##### SAMPLES
```
SELECT fingerprint, timestamp_ms, string
			FROM loki.samples
			WHERE fingerprint IN (7975981685167825999) AND timestamp_ms >= 1514730532900 AND timestamp_ms <= 1514730532902
			ORDER BY fingerprint, timestamp_ms
```
```
SELECT fingerprint, timestamp_ms, value
				FROM loki.samples
				ANY INNER JOIN 7975981685167825999 USING fingerprint
				WHERE timestamp_ms >= 1514730532900 AND timestamp_ms <= 1514730532902
				ORDER BY fingerprint, timestamp_ms
```			


#### INSERT
##### FINGERPRINTS
```
INSERT INTO loki.time_series (date, fingerprint, labels) VALUES (?, ?, ?) 
```
##### SAMPLES
```
INSERT INTO loki.samples (fingerprint, timestamp_ms, value, string) VALUES (?, ?, ?, ?)
```

