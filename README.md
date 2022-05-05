<img src='https://user-images.githubusercontent.com/1423657/147935343-598c7dfd-1412-4bad-9ac6-636994810443.png' style="margin-left:-10px" width=220>

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/lmangani/lmangani%2FcLoki%2FcLoki?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NTkxMzIxNGZlNjQxOWIwMDA2OWY1ZjU4.s1Y7vvE73ZWAIGYb4YCkATleW61RZ8sKypOc8Vae1c0&type=cf-1)]( https://g.codefresh.io/pipelines/cLoki/builds?repoOwner=lmangani&repoName=cLoki&serviceName=lmangani%2FcLoki&filter=trigger:build~Build;branch:master;pipeline:5cdf4a833a13130275ac87a8~cLoki)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)

# cLoki

### like Loki, but for ClickHouse

**cLoki** is a flexible [Loki](https://github.com/grafana/loki) [^1] compatible **LogQL API** built on top of [ClickHouse](https://clickhouse.com/)<br/>
- Built in [Explore UI](https://github.com/metrico/cloki-view) and [LogQL CLI](https://github.com/lmangani/vLogQL) for querying and extracting data
- Native support for [Grafana](http://docs.grafana.org/features/explore/) [^3] and any [LogQL](https://grafana.com/docs/loki/latest/logql/) clients for [querying](https://github.com/lmangani/cLoki/wiki/LogQL-for-Beginners), [processing](https://github.com/lmangani/cLoki/wiki/LogQL-Supported-Queries), [ingesting](https://github.com/lmangani/cLoki/wiki/Inserting-Logs-to-cLoki), [tracing](https://github.com/lmangani/cLoki/wiki/Tempo-Tracing) and [alerting](https://github.com/lmangani/cLoki/wiki/Ruler---Alerts) [^2] 
- Powerful pipeline parsers to dynamically search, filter and extract values or tags from logs, events, traces _and beyond_
- Ingestion and PUSH APIs transparently compatible with LogQL, PromQL, InfluxDB, Elastic _and more_
- Natively support in Agents such as Promtail, Grafana-Agent, Vector, Logstash, Telegraf and _many others_
- Cloud native, stateless and compact design
<br>

:octocat: Get started using the [cLoki Wiki](https://github.com/lmangani/cLoki/wiki) :bulb: 


![ezgif com-optimize 15](https://user-images.githubusercontent.com/1423657/50496835-404e6480-0a33-11e9-87a4-aebb71a668a7.gif)


### Project Background

The *Loki API* and its Grafana native integration are brilliant, simple and appealing - but we just love **ClickHouse**. 

**cLoki** implements a complete LogQL API buffered by a fast bulking **LRU** sitting on top of **ClickHouse** tables and relying on its *columnar search and insert performance alongside solid distribution and clustering capabilities* for stored data. Just like Loki, cLoki does not parse or index incoming logs, but rather groups log streams using the same label system as Prometheus. [^2]

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

cLoki implements custom query functions for ClickHouse timeseries extraction, allowing direct access to any existing table

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


--------

### :fire: Tempo: Supported Features

**cLoki Pulse** offers experimental support for the Grafana [Tempo API](https://github.com/lmangani/cLoki/wiki/Tempo-Tracing) providing span ingestion and querying

At database level, Tempo Spans/Traces are stored as tagged Logs and are accessible from both LogQL and Tempo APIs

<img src="https://user-images.githubusercontent.com/1423657/147878090-a7630467-433e-4912-a439-602ce719c21d.png" width=700 />

------------




### Setup

Check out the [Wiki](https://github.com/lmangani/cLoki/wiki) for detailed instructions or choose a quick method:

##### :busstop: GIT (Manual)
Clone this repository, install with `npm`and run using `nodejs` 14.x *(or higher)*
```bash
npm install
CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" CLICKHOUSE_DB="cloki" node cloki.js
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

#### Logging
The project uses [pino](https://github.com/pinojs/pino) for logging and by default outputs JSON'ified log lines. If you want to see "pretty" log lines you can start cloki with `npm run pretty`

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
| CLOKI_LOGIN           | undefined             | Basic HTTP Username           |
| CLOKI_PASSWORD        | undefined             | Basic HTTP Password           |
| READONLY  			| false  	    | Readonly Mode, no DB Init  		|
| FASTIFY_BODYLIMIT | 5242880   | API Maximum payload size in bytes |
| FASTIFY_REQUESTTIMEOUT | 0 | API Maximum Request Timeout in ms |
| FASTIFY_MAXREQUESTS | 0 | API Maximum Requests per socket |
| TEMPO_SPAN | 24 | Default span for Tempo queries in hours |
| TEMPO_TAGTRACE | false | Optional tagging of TraceID (expensive) |
| DEBUG  			| false  	    | Debug Mode (for backwards compatibility) 		|
| LOG_LEVEL  			| info  	    | Log Level  		|
| HASH | short-hash | Hash function using for fingerprints. Currently supported `short-hash` and `xxhash64` (xxhash64 function)


------------

#### Contributors

<a href="https://github.com/lmangani/cloki/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=lmangani/cloki" />
</a>

#### Disclaimer

©️ QXIP BV, released under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.

[^1]: cLoki is not affiliated or endorsed by Grafana Labs or ClickHouse Inc. All rights belong to their respective owners.

[^2]: cLoki is a 100% clear-room api implementation and does not fork, use or derivate from Grafana Loki code or concepts.

[^3]: Grafana®, Loki™ and Tempo® are a Trademark of Raintank, Grafana Labs. ClickHouse® is a trademark of ClickHouse Inc. Prometheus is a trademark of The Linux Foundation.
