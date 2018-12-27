<img src='https://user-images.githubusercontent.com/1423657/50455638-a8c41580-094f-11e9-8b43-dd0a9ae0f622.png' width=100>

# cLoki
#### like Loki, but for Clickhouse.

Super experimental [Loki](https://github.com/grafana/loki) emulator based on [PromHouse](https://github.com/Percona-Lab/PromHouse) schema. Do not use this!

##### Just.. Why?
The only purpose of this project is to research and understand inner aspects of the original implementation.

------------

##### API
Loki API Functions are loosely implemented as documented by the [Loki API](https://github.com/grafana/loki/blob/master/docs/api.md) reference.

* [x] /api/prom/push
* [x] /api/prom/query
* [x] /api/prom/label
* [x] /api/prom/label/_name_/values

##### Status
* [x] Basic Writes
  * [x] Label Fingerprints
  * [x] Sample Series
* [x] Basic Fingerprinting
* [x] Basic Search
  * [x] Labels  _(wildcard)_
  * [x] Samples  _(wildcard)_

--------------

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

### API Examples
```
# curl --header "Content-Type: application/json"   --request POST   --data '{"streams":[{"labels":"{\"__name__\":\"up\"}","entries":[{"timestamp":"2018-12-26T16:00:06.944Z","line":"zzz"}]}   http://localhost:3100/api/prom/push

# curl 'localhost:3100/api/prom/query?query={__name__="up"}'
{"streams":[{"labels":"{\"__name__\":\"up\"}","entries":[{"timestamp":"1545840006944","line":"zzz"},{"timestamp":"1545840006944","line":"zzz"},{"timestamp":"1545840006944","line":"zzz"}]}]}root@de4 ~ #

# curl 'localhost:3100/api/prom/label'
{"values":["__name__"]}

# curl 'localhost:3100/api/prom/label/__name__/values'
{"values":["up"]}
```

### Query Examples
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
	WHERE fingerprint IN (7975981685167825999) AND timestamp_ms >= 1514730532900 
	AND timestamp_ms <= 1514730532902
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

