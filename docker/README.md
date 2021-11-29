<img src='https://user-images.githubusercontent.com/1423657/99822833-f9504780-2b53-11eb-8b28-99484eab6157.png' width=250>

## [cLoki](https://github.com/lmangani/cLoki) + Clickhouse

This docker compose bundle will spin up a `cLoki`, `Clickhouse`, `Grafana` and `paStash` to compose a working lab system.

#### THIS EXAMPLE IS INTENDED FOR TESTING PURPOSES!


## Components

#### Core
* cLoki 
* clickhouse-server
* pastash _(sending sample logs)_
* Grafana _(w/ loki datasource)_

#### Add-Ons
* pastash [yml](https://github.com/metrico/cloki-docker-s3/blob/main/pastash.yml)

### Setup

```bash
docker-compose up
```
