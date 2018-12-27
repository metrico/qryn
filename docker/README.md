# Homer7, heplify-server, Prometheus, Grafana Stack

#### BETA VERSION! PLEASE REPORT BUGS AND IMPROVEMENTS

--------

## Setup

```bash
docker-compose up
```

to bring up:  

* [Adminer]      localhost:8080 (root/homerSeven) //select System->PostgresSQL
* [Homer]        localhost:9080 (admin/sipcapture)
* [Grafana]      localhost:3000 (admin/admin)
* [Prometheus]   localhost:9090 (admin/admin)
* [Alertmanager] localhost:9093 (admin/admin)

When the Grafana dashboard autoprovisioning does not work for you make sure you have no old grafana volumes.

## Configuration

When you change some files inside the Prometheus or Alertmanager folder you can reload them without interruption.

#### Prometheus
```bash
curl -s -XPOST localhost:9090/-/reload -u admin:admin
```

#### Alertmanager
```bash
curl -s -XPOST localhost:9093/-/reload -u admin:admin
```

#### Service
When you need to change the docker-compose file i.e to setup smtp for Grafana:
```bash
docker-compose up -d
```
Docker will only restart the service you changed inside the docker-compose file. 
