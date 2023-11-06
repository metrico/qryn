<a href="https://qryn.dev" target="_blank">
<img src='https://user-images.githubusercontent.com/1423657/218816262-e0e8d7ad-44d0-4a7d-9497-0d383ed78b83.png' style="margin-left:-10px" width=350/>
</a>

[![CI+CD](https://github.com/metrico/qryn/actions/workflows/build_release.yml/badge.svg)](https://github.com/metrico/qryn/actions/workflows/build_release.yml)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)
<img src="https://raw.githubusercontent.com/pedromxavier/flag-badges/main/badges/UA.svg" alt="made in Ukraine">

![image](https://user-images.githubusercontent.com/1423657/232089970-c4536f16-5967-4051-85a5-8ad94fcde67c.png)

# [qryn 3.x](https://qryn.dev) :cloud: [qryn.cloud](https://qryn.cloud)

:rocket: _polyglot, lighweight, multi-standard drop-in_ **observability** framework for _**Logs, Metrics and Traces**_<br/>

> ... it's pronounced /ˈkwɪr..ɪŋ/ or just _querying_

- **Polyglot**: Use **LogQL, PromQL**, and **TempoQL** languages to query, process and alert _any data_
- **Lightweight**: Powered by **ClickHouse** OLAP Engine + **Bun** the _fast, all-in-one_ JavaScript runtime
- **Voracious**: Ingestion compatible with **Opentelemetry, Loki, Prometheus, Influx, Datadog, Elastic** _+ more_
- **Versatile**: Explore data with qryn's built-in **View Explorer** and **CLI** or _native_ **Grafana** compatibility
- **Secure**: Retain total control of data, using **ClickHouse** or **InfluxDB IOx** with **S3** object storage
- **Unmetered**: Unlimited FOSS deployments or **qryn.cloud** option with advanced features and performance
- **Indepentent**: Designed to be a stand-alone, all-in-one _Loki, Prometheus, Tempo_ drop-in alternative

<br>

## 🚀 [Get Started](https://qryn.metrico.in/#/installation)

* Setup & Deploy **qryn** _OSS_ using the [documentation](https://qryn.metrico.in/#/installation) and get help in our [Matrix room](https://matrix.to/#/#qryn:matrix.org) :octocat:
* No time? Use [qryn.cloud](https://qryn.cloud) and get polyglot in just minutes! Drop-in LGTM alternative ☁️ 

<a href="https://qryn.cloud" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/218818279-3efff74f-0191-498a-bdc4-f2650c9d3b49.gif">
</a>

<br>

<br>

## Features

💡 _**qryn** independently implements popular observability standards, protocols and query languages_

<br>

### :eye: Built-In Explorer

**qryn** ships with **view** - our zero dependency, lightweight data explorer for **Logs, Metrics** and **Traces**

<a href="https://qryn.dev" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/200136242-f4133229-ee7c-45e0-8228-8734cf56140a.gif" width=700 class=border />
</a>

<br>

### ↔️ OpenTelemetry Ingestion

⚡ **qryn** is officially integrated with [opentelemetry](https://github.com/metrico/otel-collector) supports _any log, trace or metric format_

Ingested data can be queried using any of the avialable qryn APIs _(LogQL, PromQL, TraceQL)_

<br>

### ↔️ Native Ingestion

**qryn** supports [native ingestion](https://qryn.metrico.in/#/support) for Loki, Prometheus, Tempo/Zipkin and other protocols_<br>
No opentelemetry or any other proxy required. With qryn users can _push data using any combination of supported protocols_

<br>

### 📚 Loki + LogQL

> Any Loki compatible client or application can be used with qryn out of the box

⚡ **qryn** implements the [Loki API](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) for transparent compatibility with **[LogQL](https://grafana.com/docs/loki/latest/query/)** clients<br>

The Grafana Loki datasource can be used to natively browse and query _logs_ and display extracted _timeseries_<br>

<a href="https://qryn.metrico.in/#/logs/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654073-b84a218c-6a70-49bb-a477-e8be5714e0ba.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ <br>
:eye: _No Grafana? No problem! Use View_


<br>

### 📈 Prometheus + PromQL

> Any Prometheus compatible client or application can be used with qryn out of the box

⚡ **qryn** implements the [Prometheus API](https://prometheus.io/docs/prometheus/latest/querying/api/) for transparent **[PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/)** compatibility using WASM 🏆<br>

The Grafana Prometheus datasource can be used to natively to query _metrics_ and display _timeseries_<br>

<a href="https://qryn.metrico.in/#/metrics/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654084-1f1d8a62-3fd2-4420-a2fa-57ac2872938c.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ <br>
:eye: _No Grafana? No problem! Use View_



<br>

### 🕛 Tempo + TraceQL

⚡ **qryn** implements the [Tempo API](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) for transparent compatibility with **[TraceQL](https://grafana.com/docs/tempo/latest/traceql/)** clients.<br>

> Any Tempo/Opentelemetry compatible client or application can be used with qryn out of the box

The Tempo datasource can be used to natively query _traces_ including _**TraceQL**_ and supporting _service graphs_<br>

<a href="https://qryn.metrico.in/#/telemetry/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654097-8a235253-bf5d-4937-9e78-fddf12819d44.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ <br>
:eye: _No Grafana? No problem! Use View_


<br>

### 📚 Other Vendors

**qryn** can ingest data using the [InfluxDB, DataDog, Elastic](https://qryn.metrico.in/#/support) and other vendors.


<br>

With **qryn** and **grafana** everything _just works_ right out of the box: 

- Native datasource support without any plugin or extension
- Advanced Correlation between Logs, Metrics and Traces
- Service Graphs and Service Status Panels, and all the cool features

<br>

<a href="https://qryn.dev" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/184538094-13c11500-24ef-4468-9f33-dc9d564238e3.gif" width=700 class=border />
</a>

<br>

<br>

------------

📚 Follow our team _behind the scenes_ on the [qryn blog](https://blog.qryn.dev)

------------

#### Contributions

Whether it's code, documentation or grammar, we ❤️ all contributions. Not sure where to get started?

- Join our [Matrix Channel](https://matrix.to/#/#qryn:matrix.org), and ask us any questions.
- Have a PR or idea? Request a session / code walkthrough with our team for guidance.

<br>

#### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors for @metrico/qryn](https://contributors-img.web.app/image?repo=lmangani/cloki)](https://github.com/metrico/qryn/graphs/contributors)

[![Stargazers repo roster for @metrico/qryn](https://bytecrank.com/nastyox/reporoster/php/stargazersSVG.php?user=metrico&repo=qryn)](https://github.com/metrico/qryn/stargazers)

[![Forkers repo roster for @metrico/qryn](https://bytecrank.com/nastyox/reporoster/php/forkersSVG.php?user=metrico&repo=qryn)](https://github.com/metrico/qryn/network/members)


#### License

<img src="https://camo.githubusercontent.com/473b62766b498e4f2b008ada39f1d56fb3183649f24447866e25d958ac3fd79a/68747470733a2f2f7777772e676e752e6f72672f67726170686963732f6167706c76332d3135357835312e706e67">

©️ QXIP BV, released under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.


[^1]: qryn is not affiliated or endorsed by Grafana Labs or ClickHouse Inc. All rights belong to their respective owners.

[^2]: qryn is a 100% clear-room api implementation and does not fork, use or derivate from Grafana Loki code or concepts.

[^3]: Grafana®, Loki™ and Tempo® are a Trademark of Raintank, Grafana Labs. ClickHouse® is a trademark of ClickHouse Inc. Prometheus is a trademark of The Linux Foundation.
