<img src='https://user-images.githubusercontent.com/1423657/99822833-f9504780-2b53-11eb-8b28-99484eab6157.png' width=250>

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/lmangani/lmangani%2FcLoki%2FcLoki?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NTkxMzIxNGZlNjQxOWIwMDA2OWY1ZjU4.s1Y7vvE73ZWAIGYb4YCkATleW61RZ8sKypOc8Vae1c0&type=cf-1)]( https://g.codefresh.io/pipelines/cLoki/builds?repoOwner=lmangani&repoName=cLoki&serviceName=lmangani%2FcLoki&filter=trigger:build~Build;branch:master;pipeline:5cdf4a833a13130275ac87a8~cLoki)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)

# cLoki

### like Loki, but for Clickhouse.

cLoki is a clear room design [Loki](https://github.com/grafana/loki) API emulator made with NodeJS, [Fastify](https://github.com/fastify/fastify) and [Clickhouse](https://clickhouse.yandex/)<br/>
APIs are compatible with [Grafana](http://docs.grafana.org/features/explore/), [LogQL](https://grafana.com/docs/loki/latest/logql/) and [paStash](https://github.com/sipcapture/paStash/wiki/Example:-Loki) for logs querying, processing and ingestion

Performance is comparable to native Loki, with cLoki outperforming on large range filtered queries.

:bulb: Get started using the [cLoki Wiki](https://github.com/lmangani/cLoki/wiki)

![ezgif com-optimize 15](https://user-images.githubusercontent.com/1423657/50496835-404e6480-0a33-11e9-87a4-aebb71a668a7.gif)

:fire: *Beta Stage, Contributors and Testers are Welcome!* :octocat:


### Project Background

The *Loki API* and its Grafana native integration are brilliant, simple and appealing - but we just love **Clickhouse**. 

**cLoki** implements the same API functionality as Loki, buffered by a fast bulking **LRU** sitting on top of **Clickhouse** tables and relying on its *columnar search and insert performance alongside solid distribuion and clustering capabilities* for stored data. Just like Loki, cLoki does not parse or index incoming logs, but rather groups log streams using the same label system as Prometheus. 

<img src="https://user-images.githubusercontent.com/1423657/54091852-5ce91000-4385-11e9-849d-998c1e5d3243.png" width=700 />

### :fire: CliQL: Experimental 2.0 Features

cLoki implements custom query functions for clickhouse timeseries extraction, allowing direct access to any table

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

#### Telegraf
Insert using [Telegraf Input](https://github.com/lmangani/cLoki/wiki/Telegraf-HTTP-Input) and display metrics and logs in Grafana without plugins


------------
### Setup

##### :busstop: GIT (Manual)
Clone this repository, install with `npm`and run using `nodejs` 12.x *(or higher)*
```
npm install
CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" node ./cloki.js
```
##### :busstop: NPM
Install `cloki` as global package on your system using `npm`
```
sudo npm install -g cloki
cd $(dirname $(readlink -f `which cloki`)) && CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" cloki
```
##### :busstop: PM2
```
sudo npm install -g cloki pm2
cd $(dirname $(readlink -f `which cloki`)) && CLICKHOUSE_SERVER="my.clickhouse.server" CLICKHOUSE_AUTH="default:password" pm2 start cloki
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
| CLICKHOUSE_DB  	| default  	    | Clickhouse Database Name  		|
| CLICKHOUSE_TSDB  	| loki  	    | Clickhouse TS Database Name  		|
| CLICKHOUSE_AUTH  	| default:  	    | Clickhouse Authentication (user:password) |
| TIMEFIELD | record_datetime | Clickhouse DateTime column for native queries |
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
| DEBUG  			| false  	    | Debug Mode  		|

#### :fuelpump: Log Streams

The ideal companion for parsing and shipping log streams to **cLoki** is [paStash](https://github.com/sipcapture/paStash/wiki/Example:-Loki) with extensive interpolation capabilities.

------------

### Project Status

##### API

Loki API Functions are loosely implemented as documented by the [Loki API](https://github.com/grafana/loki/blob/master/docs/api.md) reference.

* [x] /loki/api/v1/push
* [x] /loki/api/v1/query
* [x] /loki/api/v1/query_range
* [x] /loki/api/v1/label
* [x] /loki/api/v1/label/_name_/values

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
  * [x] =~ regex-match.
  * [x] !~ do not regex-match.
* [x] Basic Search
  * [x] Labels  _(single key, multi key, AND logic)_
  * [x] Samples  _(by Fingerprint match)_

--------------


#### Acknowledgements
cLoki is not affiliated or endorsed by Grafana Labs. All rights belong to their respective owners.
