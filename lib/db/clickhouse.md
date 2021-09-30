### Clickhouse Database Schema

```sql
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

### Raw Queries

#### CREATE

###### DATABASE

```sql
CREATE DATABASE IF NOT EXISTS loki
```

###### TABLES

```sql
CREATE TABLE IF NOT EXISTS loki.time_series (
    date Date,
    fingerprint UInt64,
    labels String,
    name String
) ENGINE = ReplacingMergeTree PARTITION BY date ORDER BY fingerprint;

CREATE TABLE IF NOT EXISTS loki.samples (
    fingerprint UInt64,
    timestamp_ms Int64,
    value Float64,
    string String
) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (fingerprint, timestamp_ms);
```

#### SELECT

###### FINGERPRINTS

```sql
SELECT DISTINCT fingerprint, labels FROM loki.time_series
```

###### SAMPLES

```sql
SELECT fingerprint, timestamp_ms, string
FROM loki.samples
WHERE fingerprint IN (7975981685167825999) AND timestamp_ms >= 1514730532900 
AND timestamp_ms <= 1514730532902
ORDER BY fingerprint, timestamp_ms
```

```sql
SELECT fingerprint, timestamp_ms, value
FROM loki.samples
ANY INNER JOIN 7975981685167825999 USING fingerprint
WHERE timestamp_ms >= 1514730532900 AND timestamp_ms <= 1514730532902
ORDER BY fingerprint, timestamp_ms
```			

#### INSERT
###### FINGERPRINTS
```sql
INSERT INTO loki.time_series (date, fingerprint, labels, name) VALUES (?, ?, ?, ?) 
```
###### SAMPLES
```sql
INSERT INTO loki.samples (fingerprint, timestamp_ms, value, string) VALUES (?, ?, ?, ?)
```

------------
