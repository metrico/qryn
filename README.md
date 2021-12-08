<img src='https://user-images.githubusercontent.com/1423657/139434383-98287329-74ce-4061-aabb-a19e500a986c.png' width=250>

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/lmangani/lmangani%2FcLoki%2FcLoki?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NTkxMzIxNGZlNjQxOWIwMDA2OWY1ZjU4.s1Y7vvE73ZWAIGYb4YCkATleW61RZ8sKypOc8Vae1c0&type=cf-1)]( https://g.codefresh.io/pipelines/cLoki/builds?repoOwner=lmangani&repoName=cLoki&serviceName=lmangani%2FcLoki&filter=trigger:build~Build;branch:master;pipeline:5cdf4a833a13130275ac87a8~cLoki)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)

# cLoki

### like Loki, but for Clickhouse.

cLoki is a clear room design [Loki](https://github.com/grafana/loki) API emulator made with NodeJS, [Fastify](https://github.com/fastify/fastify) and [Clickhouse](https://clickhouse.yandex/)<br/>
APIs are compatible with [Grafana](http://docs.grafana.org/features/explore/) and [LogQL](https://grafana.com/docs/loki/latest/logql/) clients for [querying](https://github.com/lmangani/cLoki/wiki/LogQL-for-Beginners), [processing](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries), [ingesting](https://github.com/lmangani/cLoki/wiki/Inserting-Logs-to-cLoki) logs and events

Performance is comparable to native Loki, with cLoki outperforming on large range filtered queries.

:bulb: Get started using the [cLoki Wiki](https://github.com/lmangani/cLoki/wiki)<br>


![ezgif com-optimize 15](https://user-images.githubusercontent.com/1423657/50496835-404e6480-0a33-11e9-87a4-aebb71a668a7.gif)

:fire: *Beta Stage, Contributors and Testers are Welcome!* :octocat:


### Project Background

The *Loki API* and its Grafana native integration are brilliant, simple and appealing - but we just love **Clickhouse**. 

**cLoki** implements the same API functionality as Loki, buffered by a fast bulking **LRU** sitting on top of **Clickhouse** tables and relying on its *columnar search and insert performance alongside solid distribution and clustering capabilities* for stored data. Just like Loki, cLoki does not parse or index incoming logs, but rather groups log streams using the same label system as Prometheus. 

<img src="https://user-images.githubusercontent.com/1423657/54091852-5ce91000-4385-11e9-849d-998c1e5d3243.png" width=700 />

### :fire: LogQL: Supported Features

cLoki implements a broad range of [LogQL Queries](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries) to provide transparent compatibility with the Loki API<br>
The Grafana Loki datasource can be used to natively query _logs_ and display extracted _timeseries_<br>

:tada: _No plugins needed_ 

<img src="https://user-images.githubusercontent.com/1423657/135249640-5f5a61e5-0f94-4517-b052-76d47c3572f5.png" height=100>

- [Log Stream Selector](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#log-stream-selector)
- [Line Filter Expression](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#line-filter-expression)
- [Label Filter Expression](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#label-filter-expression)
- [Parser Expression](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#parser-expression)
- [Log Range Aggregations](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#log-range-aggregations)
- [Aggregation operators](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#aggregation-operators)
- [Unwrap Expression.](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#unwrap-expression)
- [Line Format Expression](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries#line-format-expression---handlebars--)

:fire: Follow our [examples](https://github.com/lmangani/cLoki/wiki/LogQL-for-Beginners) to get started

--------

### :fuelpump: Log Streams

cLoki supports input via Push API using *JSON* or *Protobuf* and it is compatible with [Promtail](https://grafana.com/docs/loki/latest/clients/promtail/) and any other [Loki compatible agent](https://github.com/lmangani/cLoki/wiki/Inserting-Logs-to-cLoki)

Our _preferred_ companion for parsing and shipping log streams to **cLoki** is [paStash](https://github.com/sipcapture/paStash/wiki/Example:-Loki) with extensive interpolation capabilities to create tags and trim any log fat. Sending JSON formatted logs is _suggested_ when dealing with metrics.

--------

### :fire: CliQL: Experimental 2.0 Features

cLoki implements custom query functions for clickhouse timeseries extraction, allowing direct access to any existing table

![ezgif com-gif-maker](https://user-images.githubusercontent.com/1423657/99530591-d0885080-29a1-11eb-87e6-870a046fb4de.gif)


#### Timeseries
Convert columns to tagged timeseries using the emulated loki 2.0 query format
```
<aggr-op> by (<labels,>) (<function>(<metric>[range_in_seconds])) from <database>.<table> where <optional condition>
```

###### Examples
<pre>
<b>avg</b> by (<b>source_ip</b>) (rate(<b>mos</b>[<b>60</b>])) from <b>my_database.my_table</b>
</pre>
<pre>
<b>sum</b> by (<b>ruri_user, from_user</b>) (rate(<b>duration</b>[<b>300</b>])) from <b>my_database.my_table</b> where <b>duration > 10</b>
</pre>

#### Clickhouse
Convert columns to tagged timeseries using the experimental `clickhouse` function
#### Example
<pre>
clickhouse({ 
  db="<b>my_database</b>", 
  table="<b>my_table</b>", 
  tag="<b>source_ip</b>", 
  metric="<b>avg(mos)</b>", 
  where="<b>mos > 0</b>", 
  interval="<b>60</b>" 
})
</pre>

###### Query Options
| parameter  | description  |
|---|---|
|db       | clickhouse database name  |
|table    | clickhouse table name |
|tag      | column(s) for tags, comma separated | 
|metric   | function for metric values |
|where    | where condition (optional) |
|interval | interval in seconds (optional) |
|timefield| time/date field name (optional) |


------------
### Setup

##### :busstop: GIT (Manual)
Clone this repository, install with `npm`and run using `nodejs` 14.x *(or higher)*
```bash
npm install
CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" CLICKHOUSE_DB="cloki" node ./cloki.js
```
##### :busstop: NPM
Install `cloki` as global package on your system using `npm`
```bash
sudo npm install -g cloki
cd $(dirname $(readlink -f `which cloki`)) \
  && CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" CLICKHOUSE_DB="cloki" cloki
```
##### :busstop: PM2
```bash
sudo npm install -g cloki pm2
cd $(dirname $(readlink -f `which cloki`)) \
  && CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" CLICKHOUSE_DB="cloki" pm2 start cloki
pm2 save
pm2 startup
```

##### :busstop: Docker
For a fully working demo, check the [docker-compose](https://github.com/lmangani/cLoki/tree/master/docker) example


--------------

#### Configuration
The following ENV Variables can be used to control cLoki parameters and backend settings.

|ENV   	|Default   	|Usage   	|
|---	|---	    |---		|
| CLICKHOUSE_SERVER | localhost   	| Clickhouse Server address  		|
| CLICKHOUSE_PORT  	| 8123  	    | Clickhouse Server port  		|
| CLICKHOUSE_DB  	| cloki  	    | Clickhouse Database Name  		|
| CLICKHOUSE_AUTH  	| default:  	    | Clickhouse Authentication (user:password) |
| CLICKHOUSE_PROTO  	| http  	    | Clickhouse Protocol (http, https) |
| CLICKHOUSE_TIMEFIELD  | record_datetime    | Clickhouse DateTime column for native queries |
| BULK_MAXAGE  		| 2000  	    | Max Age for Bulk Inserts  		|
| BULK_MAXSIZE  	| 5000  	    | Max Size for Bulk Inserts  		|
| BULK_MAXCACHE  	| 50000  	    | Max Labels in Memory Cache  		|
| LABELS_DAYS  		| 7  	    	    | Max Days before Label rotation  		|
| SAMPLES_DAYS  	| 7  	    	    | Max Days before Timeseries rotation  		|
| HOST 			| 0.0.0.0 	    | cLOKi API IP  		|
| PORT  		| 3100 	            | cLOKi API PORT  		|
| CLOKI_LOGIN           | false             | Basic HTTP Username           |
| CLOKI_PASSWORD        | false             | Basic HTTP Password           |
| READONLY  			| false  	    | Readonly Mode, no DB Init  		|
| FASTIFY_BODYLIMIT | 5242880   | API Maximum payload size in bytes |
| FASTIFY_REQUESTTIMEOUT | 0 | API Maximum Request Timeout in ms |
| FASTIFY_MAXREQUESTS | 0 | API Maximum Requests per socket |
| DEBUG  			| false  	    | Debug Mode  		|


------------

### Project Status

##### API

Loki API Functions are loosely implemented as documented by the [Loki API](https://github.com/grafana/loki/blob/master/docs/api.md) reference.

* [x] /loki/api/v1/push
* [x] /loki/api/v1/query
* [x] /loki/api/v1/query_range
* [x] /loki/api/v1/label
* [x] /loki/api/v1/label/_name_/values
* [x] /loki/api/v1/tail

##### Status

Consult the [Wiki](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries) for a detailed list of supported features

--------------


#### Acknowledgements
cLoki is not affiliated or endorsed by Grafana Labs. All rights belong to their respective owners.
