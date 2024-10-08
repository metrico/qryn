# gigaswap - long test

long-term test of qryn endpoint
It sends 
- 3000 logs/sec
- 3000 influx logs / sec
- 300 zipkin traces/sec
- 300 datadog traces/sec with variable amount of spans
- 5K of metrics / 15 sec

depending on the MODE until you stop it.

# Usage

- go build -o longtest
- URL='<base url like http://localhost:1234>' MODE=<MODE LIST LMZDIC> ZPATH=/path/for/zipkin DPATH=/path/for/datadog ./longtest

## MODE LIST

- L - for loki logs
- M - for metrics remote-write
- Z - for zipkin traces
- D - for datadog traces
- I - for influx logs
- C - for a lot of small simultaneous loki log request to check batching
- S - for servicegraph testing
