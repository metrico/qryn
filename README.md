<img src='https://user-images.githubusercontent.com/1423657/173144443-fc7ba783-d5bf-47f9-bf59-707693da5ed1.png' style="margin-left:-10px" width=120/><img src="https://user-images.githubusercontent.com/1423657/173145328-dc9cc3b0-85fc-49d9-8e8a-5128a363408f.png" width=400/>


[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/lmangani/lmangani%2FcLoki%2FcLoki?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NTkxMzIxNGZlNjQxOWIwMDA2OWY1ZjU4.s1Y7vvE73ZWAIGYb4YCkATleW61RZ8sKypOc8Vae1c0&type=cf-1)]( https://g.codefresh.io/pipelines/cLoki/builds?repoOwner=lmangani&repoName=cLoki&serviceName=lmangani%2FcLoki&filter=trigger:build~Build;branch:master;pipeline:5cdf4a833a13130275ac87a8~cLoki)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)

# qryn / cLogQL

### LogQL for ClickHouse

**qryn** is a flexible **LogQL API** built on top of [ClickHouse](https://clickhouse.com/) and natively integrated in [Uptrace](https://uptrace.dev)<br/>
- Built in [Explore UI](https://github.com/metrico/cloki-view) and [LogQL CLI](https://github.com/lmangani/vLogQL) for querying and extracting data
- Native [Grafana](http://docs.grafana.org/features/explore/) [^3] and [LogQL](https://grafana.com/docs/loki/latest/logql/) APIs for [querying](https://github.com/lmangani/qryn/wiki/LogQL-for-Beginners), [processing](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries), [ingesting](https://github.com/lmangani/qryn/wiki/Inserting-Logs-to-cLoki), [tracing](https://github.com/lmangani/qryn/wiki/Tempo-Tracing) and [alerting](https://github.com/lmangani/qryn/wiki/Ruler---Alerts) [^2] 
- Powerful pipeline to dynamically search, filter and extract data from logs, events, traces _and beyond_
- Ingestion and PUSH APIs transparently compatible with LogQL, PromQL, InfluxDB, Elastic _and more_
- Ready to use with Agents such as Promtail, Grafana-Agent, Vector, Logstash, Telegraf and _many others_
- Cloud native, stateless and compact design
<br>

:octocat: Get started using our [Wiki](https://github.com/lmangani/qryn/wiki) :bulb: 

⚠️ Existing user and confused? The project has been renamed to `qryn` _(querying)_ 👍 


![ezgif com-optimize 15](https://user-images.githubusercontent.com/1423657/50496835-404e6480-0a33-11e9-87a4-aebb71a668a7.gif)


### Project Background

**qryn** implements a complete LogQL API buffered by a fast bulking **LRU** sitting on top of **ClickHouse** tables and relying on its *columnar search and insert performance alongside solid distribution and clustering capabilities* for stored data. qryn does not parse or index incoming logs, but rather groups log streams using the same label system as Prometheus. [^2]

<img src="https://user-images.githubusercontent.com/1423657/54091852-5ce91000-4385-11e9-849d-998c1e5d3243.png" width=700 />

### :fire: LogQL: Supported Features

qryn implements a broad range of [LogQL Queries](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) to provide transparent compatibility with the Loki API<br>
The Grafana Loki datasource can be used to natively query _logs_ and display extracted _timeseries_<br>

:tada: _No plugins needed_ 

<img src="https://user-images.githubusercontent.com/1423657/135249640-5f5a61e5-0f94-4517-b052-76d47c3572f5.png" height=100>

- [Log Stream Selector](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries#log-stream-selector)
- [Line Filter Expression](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries#line-filter-expression)
- [Label Filter Expression](https://github.com/lmangani/cLqrynoki/wiki/LogQL-Supported-Queries#label-filter-expression)
- [Parser Expression](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries#parser-expression)
- [Log Range Aggregations](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries#log-range-aggregations)
- [Aggregation operators](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries#aggregation-operators)
- [Unwrap Expression.](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries#unwrap-expression)
- [Line Format Expression](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries#line-format-expression---handlebars--)

:fire: Follow our [examples](https://github.com/lmangani/qryn/wiki/LogQL-for-Beginners) to get started

--------

### :fuelpump: Log Streams

qryn supports input via Push API using *JSON* or *Protobuf* and it is compatible with [Promtail](https://grafana.com/docs/loki/latest/clients/promtail/) and any other [LogQL compatible agent](https://github.com/lmangani/qryn/wiki/Inserting-Logs-to-cLoki). On top of that, qryn also accepts and converts log and metric inserts using Influx, Elastic, Tempo and other common API formats.

Our _preferred_ companion for parsing and shipping log streams to **qryn** is [paStash](https://github.com/sipcapture/paStash/wiki/Example:-Loki) with extensive interpolation capabilities to create tags and trim any log fat. Sending JSON formatted logs is _suggested_ when dealing with metrics.

--------

### :fire: CliQL: Experimental 2.0 Features

qryn implements custom query functions for ClickHouse timeseries extraction, allowing direct access to any existing table

![ezgif com-gif-maker](https://user-images.githubusercontent.com/1423657/99530591-d0885080-29a1-11eb-87e6-870a046fb4de.gif)


#### Timeseries
Convert columns to tagged timeseries using the emulated LogQL 2.0 query format
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

#### ClickHouse
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



### Setup

Check out the [Wiki](https://github.com/lmangani/qryn/wiki) for detailed instructions or choose a quick method:

##### :busstop: GIT (Manual)
Clone this repository, install with `npm`and run using `nodejs` 14.x *(or higher)*
```bash
npm install
CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" CLICKHOUSE_DB="qryn" node qryn.js
```
##### :busstop: NPM
Install `qryn` as global package on your system using `npm`
```bash
sudo npm install -g qryn
cd $(dirname $(readlink -f `which qryn`)) \
  && CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" CLICKHOUSE_DB="qryn" qryn
```
##### :busstop: PM2
```bash
sudo npm install -g qryn pm2
cd $(dirname $(readlink -f `which qryn`)) \
  && CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" CLICKHOUSE_DB="qryn" pm2 start qryn
pm2 save
pm2 startup
```

##### :busstop: Docker
For a fully working demo, check the [docker-compose](https://github.com/lmangani/qryn/tree/master/docker) example


--------------

#### Logging
The project uses [pino](https://github.com/pinojs/pino) for logging and by default outputs JSON'ified log lines. If you want to see "pretty" log lines you can start qryn with `npm run pretty`

#### Configuration
The following ENV Variables can be used to control qryn parameters and backend settings.

| ENV   	                |Default   	|Usage   	|
|------------------------|---	    |---		|
| CLICKHOUSE_SERVER      | localhost   	| Clickhouse Server address  		|
| CLICKHOUSE_PORT  	     | 8123  	    | Clickhouse Server port  		|
| CLICKHOUSE_DB  	       | qryn  	    | Clickhouse Database Name  		|
| CLICKHOUSE_AUTH  	     | default:  	    | Clickhouse Authentication (user:password) |
| CLICKHOUSE_PROTO  	    | http  	    | Clickhouse Protocol (http, https) |
| CLICKHOUSE_TIMEFIELD   | record_datetime    | Clickhouse DateTime column for native queries |
| BULK_MAXAGE  		        | 2000  	    | Max Age for Bulk Inserts  		|
| BULK_MAXSIZE  	        | 5000  	    | Max Size for Bulk Inserts  		|
| BULK_MAXCACHE  	       | 50000  	    | Max Labels in Memory Cache  		|
| LABELS_DAYS  		        | 7  	    	    | Max Days before Label rotation  		|
| SAMPLES_DAYS  	        | 7  	    	    | Max Days before Timeseries rotation  		|
| HOST 			               | 0.0.0.0 	    | HTTP API IP  		|
| PORT  		               | 3100 	            | HTTP API PORT  		|
| QRYN_LOGIN              | undefined             | Basic HTTP Username           |
| QRYN_PASSWORD         | undefined             | Basic HTTP Password           |
| READONLY  			          | false  	    | Readonly Mode, no DB Init  		|
| FASTIFY_BODYLIMIT      | 5242880   | API Maximum payload size in bytes |
| FASTIFY_REQUESTTIMEOUT | 0 | API Maximum Request Timeout in ms |
| FASTIFY_MAXREQUESTS    | 0 | API Maximum Requests per socket |
| TEMPO_SPAN             | 24 | Default span for Tempo queries in hours |
| TEMPO_TAGTRACE         | false | Optional tagging of TraceID (expensive) |
| DEBUG  			             | false  	    | Debug Mode (for backwards compatibility) 		|
| LOG_LEVEL  			         | info  	    | Log Level  		|
| HASH                   | short-hash | Hash function using for fingerprints. Currently supported `short-hash` and `xxhash64` (xxhash64 function)


------------

#### Contributors

<a href="https://github.com/lmangani/qryn/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=lmangani/cloki" />
</a>

#### Disclaimer

©️ QXIP BV, released under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.

[^1]: qryn is not affiliated or endorsed by Grafana Labs or ClickHouse Inc. All rights belong to their respective owners.

[^2]: qryn is a 100% clear-room api implementation and does not fork, use or derivate from Grafana Loki code or concepts.

[^3]: Grafana®, Loki™ and Tempo® are a Trademark of Raintank, Grafana Labs. ClickHouse® is a trademark of ClickHouse Inc. Prometheus is a trademark of The Linux Foundation.
