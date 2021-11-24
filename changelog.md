Release 1.1.10, 2021-11-18
- Added `NO_SNAPPY=1` environment variable to suppress Snappy due to the centos 7 requirements
Release 1.1.11, 2021-11-18
- Removed `NO_SNAPPY=1` environment variable. Replaced by the automatic dependency check

Release 1.1.17, 2021-11-23
- clustered clickhouse support

Env variables to configure clustered clickhouse:
```
CLICKHOUSE_SERVER=clickhouse_server1_hostname:8123;clickhouse_server2_hostname:8123
CLICKHOUSE_CLUSTERED=cloki_distributed_cluster_name_from_conf_xml
```
