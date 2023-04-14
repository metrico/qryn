<a href="https://qryn.dev" target="_blank">
<img src='https://user-images.githubusercontent.com/1423657/218816262-e0e8d7ad-44d0-4a7d-9497-0d383ed78b83.png' style="margin-left:-10px" width=350/>
</a>

[![Build Status](https://github.com/metrico/qryn/actions/workflows/bump_version.yml/badge.svg)](https://github.com/metrico/qryn/actions/workflows/bump_version.yml)
![CodeQL](https://github.com/lmangani/cLoki/workflows/CodeQL/badge.svg)
<a href="https://matrix.to/#/#qryn:matrix.org">
  <img src="https://img.shields.io/badge/Join%20Matrix-Chat-green" alt="Matrix">
</a>


# [qryn.dev](https://qryn.dev) :cloud: [qryn.cloud](https://qryn.cloud) :heart:
> ... it's pronounced /ˈkwɪr..ɪŋ/ or just querying

![image](https://user-images.githubusercontent.com/1423657/232089970-c4536f16-5967-4051-85a5-8ad94fcde67c.png)


:rocket: **qryn** is a drop-in compatible **polyglot observability** framework built on top of [ClickHouse](https://clickhouse.com/)<br/>
- All your **Logs, Metrics and Traces**. Compatible with multiple standards, with native correlation, shared labels
- Ingestion and [PUSH APIs](https://qryn.metrico.in/#/support) transparently compatible with [LogQL, PromQL, InfluxDB, Elastic](https://qryn.dev) _and many more_
- Native [Grafana](http://docs.grafana.org/features/explore/) [^3] and [LogQL/Tempo/Prometheus APIs](https://qryn.metrico.in/#/support) for [querying](https://github.com/lmangani/qryn/wiki/LogQL-for-Beginners), [processing](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries), [ingesting](https://github.com/lmangani/qryn/wiki/Inserting-Logs-to-cLoki), [tracing](https://github.com/lmangani/qryn/wiki/Tempo-Tracing) and [alerting](https://github.com/lmangani/qryn/wiki/Ruler---Alerts) [^2] 
- Powerful pipelines to dynamically search, filter and extract metrics from _logs, events, spans and traces_. _NO SQL required_.
- Ready to use with popular Agents such as [Promtail, Grafana-Agent, Vector, Logstash, Telegraf](https://qryn.metrico.in/#/ingestion) and _many others_
- Built in [Explore UI](https://github.com/metrico/cloki-view) and [CLI](https://github.com/lmangani/vLogQL) for querying supported datasources
- [Cloud native](https://qryn.cloud), object storage friendly, stateless and compact.
- Designed for both edge _(js/wasm)_ and core/backend deployments _(go/rust)_.
<br>

<a href="https://qryn.dev" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/218818279-3efff74f-0191-498a-bdc4-f2650c9d3b49.gif">
</a>

<br>

## 🚀 [Get Started](https://qryn.metrico.in/#/installation)

:octocat: Get qryn up and running in no time using our [Documentation](https://qryn.metrico.in/#/installation) or join our [Matrix Room](https://matrix.to/#/#qryn:matrix.org)


<br>

## Supported Features

### 📚 LogQL
qryn implements a complete [LogQL API](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) to provide transparent compatibility with Loki clients<br>
The Grafana Loki datasource can be used to natively browse and query _logs_ and display extracted _timeseries_<br>

<a href="https://qryn.metrico.in/#/logs/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654073-b84a218c-6a70-49bb-a477-e8be5714e0ba.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ 

<br>

### 📈 Prometheus
qryn implements a complete [Prometheus API](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) to provide transparent compatibility with Prometheus clients<br>
The Grafana Prometheus datasource can be used to natively browse and query _metrics_ and display extracted _timeseries_<br>

<a href="https://qryn.metrico.in/#/metrics/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654084-1f1d8a62-3fd2-4420-a2fa-57ac2872938c.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ 

<br>

### 🕛 Tempo
qryn implements the [Tempo API](https://github.com/lmangani/qryn/wiki/LogQL-Supported-Queries) to provide transparent compatibility with Tempo/OTLP clients.<br>
The Tempo datasource can be used to natively query _traces_ including _beta search_ and _service graphs_<br>

<a href="https://qryn.metrico.in/#/telemetry/query" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/196654097-8a235253-bf5d-4937-9e78-fddf12819d44.gif" width=700 class=border />
</a>

:tada: _No plugins needed_ 

<br>

### ↔️ Correlation
Data correlation made simple with dynamic **links** between _logs, metrics and traces_

<a href="https://qryn.dev" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/184538094-13c11500-24ef-4468-9f33-dc9d564238e3.gif" width=700 class=border />
</a>

<br>

### :eye: View

No Grafana? No Problem. **qryn** ships with **view** - it's own lightweight data exploration tool

<a href="https://qryn.dev" target="_blank">
<img src="https://user-images.githubusercontent.com/1423657/200136242-f4133229-ee7c-45e0-8228-8734cf56140a.gif" width=700 class=border />
</a>

------------

📚 Follow our team _behind the scenes_ on the [qryn blog](https://blog.qryn.dev)

------------

#### Contributors

&nbsp;&nbsp;&nbsp;&nbsp;[![Contributors for @metrico/qryn](https://contributors-img.web.app/image?repo=lmangani/cloki)](https://github.com/metrico/qryn/graphs/contributors)

[![Stargazers repo roster for @metrico/qryn](https://reporoster.com/stars/metrico/qryn)](https://github.com/metrico/qryn/stargazers)

[![Forkers repo roster for @metrico/qryn](https://reporoster.com/forks/metrico/qryn)](https://github.com/metrico/qryn/network/members)


#### License

<img src="https://camo.githubusercontent.com/473b62766b498e4f2b008ada39f1d56fb3183649f24447866e25d958ac3fd79a/68747470733a2f2f7777772e676e752e6f72672f67726170686963732f6167706c76332d3135357835312e706e67">

©️ QXIP BV, released under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for details.

We encourage forking and changing the code, hacking around with it, and experimenting. If you modify the qryn source code, and run that modified code in a way that's accessible over a network, you _must_ make your modifications to the source code available following the guidelines of the license:
```
[I]f you modify the Program, your modified version must prominently offer all users interacting with it remotely 
through a computer network (if your version supports such interaction) an opportunity to receive the Corresponding 
Source of your version by providing access to the Corresponding Source from a network server at no charge, through 
some standard or customary means of facilitating copying of software.
```


[^1]: qryn is not affiliated or endorsed by Grafana Labs or ClickHouse Inc. All rights belong to their respective owners.

[^2]: qryn is a 100% clear-room api implementation and does not fork, use or derivate from Grafana Loki code or concepts.

[^3]: Grafana®, Loki™ and Tempo® are a Trademark of Raintank, Grafana Labs. ClickHouse® is a trademark of ClickHouse Inc. Prometheus is a trademark of The Linux Foundation.
