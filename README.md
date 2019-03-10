<img src='https://user-images.githubusercontent.com/1423657/50455638-a8c41580-094f-11e9-8b43-dd0a9ae0f622.png' width=100>

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/lmangani/lmangani%2Fcloki%2Fcloki?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NTkxMzIxNGZlNjQxOWIwMDA2OWY1ZjU4.s1Y7vvE73ZWAIGYb4YCkATleW61RZ8sKypOc8Vae1c0&type=cf-1)]( https://g.codefresh.io/pipelines/cloki/builds?repoOwner=lmangani&repoName=cloki&serviceName=lmangani%2Fcloki&filter=trigger:build~Build;branch:master;pipeline:5c255ae6ada6ff8dbbfd489b~cloki)

# cLoki
#### like Loki, but for Clickhouse.

Super experimental, fully functional [Loki](https://github.com/grafana/loki) API emulator made with NodeJS, [Fastify](https://github.com/fastify/fastify) and [Clickhouse](https://clickhouse.yandex/). 

* Compatible with Grafana Explore and [paStash](https://github.com/sipcapture/paStash/wiki/Example:-Loki) for data ingestion

*Do not use this for anything serious.. yet!*

![ezgif com-optimize 15](https://user-images.githubusercontent.com/1423657/50496835-404e6480-0a33-11e9-87a4-aebb71a668a7.gif)

##### Just.. Why?
The Loki API is brilliantly simple and appealing - its misteriously assembled backend, not so much. cLoki implements the same API functionality buffered on top of a bulking LRU sitting on top of Clickhouse and relying on its performance, distribuion and clustering capabilities for stored data.

<img src="https://user-images.githubusercontent.com/1423657/54091852-5ce91000-4385-11e9-849d-998c1e5d3243.png" width=700>

*The current purpose of this project is to research and understand inner aspects of the original implementation.*


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
  * [x] JSON Support
  * [ ] ProtoBuf Support
* [x] Basic Fingerprinting
* [ ] Stream Selector rules _()_
  * [x] = exactly equal.
  * [x] != not equal.
  * [ ] =~ regex-match.
  * [ ] !~ do not regex-match.
* [x] Basic Search
  * [x] Labels  _(single key, multi key, AND logic)_
  * [x] Samples  _(by Fingerprint match)_

--------------

### Database Schema
```
CREATE TABLE time_series (
    date Date,
    fingerprint UInt64,
    labels String,
    name String
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
# curl --header "Content-Type: application/json" --request POST \
  --data '{"streams":[{"labels":"{\"__name__\":\"up\"}","entries":[{"timestamp":"2018-12-26T16:00:06.944Z","line":"zzz"}]} \ http://localhost:3100/api/prom/push

# curl 'localhost:3100/api/prom/query?query={__name__="up"}'
{"streams":[{"labels":"{\"__name__\":\"up\"}","entries":[{"timestamp":"1545840006944","line":"zzz"},{"timestamp":"1545840006944","line":"zzz"},{"timestamp":"1545840006944","line":"zzz"}]}]}root@de4 ~ #

# curl 'localhost:3100/api/prom/label'
{"values":["__name__"]}

# curl 'localhost:3100/api/prom/label/__name__/values'
{"values":["up"]}
```

### Raw Queries

#### CREATE
##### DATABASE
```
CREATE DATABASE IF NOT EXISTS loki
```
##### TABLES
```
CREATE TABLE IF NOT EXISTS loki.time_series (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree PARTITION BY date ORDER BY fingerprint;
CREATE TABLE IF NOT EXISTS loki.samples (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (fingerprint, timestamp_ms);
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
INSERT INTO loki.time_series (date, fingerprint, labels, name) VALUES (?, ?, ?, ?) 
```
##### SAMPLES
```
INSERT INTO loki.samples (fingerprint, timestamp_ms, value, string) VALUES (?, ?, ?, ?)
```

