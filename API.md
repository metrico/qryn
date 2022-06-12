# Qryn

### API Examples

###### INSERT Labels & Logs

```console
curl -i -XPOST -H "Content-Type: application/json" http://localhost:3100/loki/api/v1/push --data '{"streams":[{"labels":"{\"__name__\":\"up\"}","entries":[{"timestamp":"2018-12-26T16:00:06.944Z","line":"zzz"}]}]}'
```

###### INSERT Labels & Metrics

```console
curl -i -XPOST -H "Content-Type: application/json" http://localhost:3100/loki/api/v1/push --data '{"streams":[{"labels":"{\"__name__\":\"metric\"}","entries":[{"timestamp":"2018-12-26T16:00:06.944Z","value":"100"}]}]}'
```

###### QUERY Logs

```console
# curl localhost:3100/loki/api/v1/query?query='{__name__="up"}'
```

```json
{
    "streams": [
        {
            "labels": "{\"__name__\":\"up\"}",
            "entries": [
                {
                    "timestamp":"1545840006944",
                    "line":"zzz"
                },
                {
                    "timestamp":"1545840006944",
                    "line":"zzz"
                },
                {
                    "timestamp": "1545840006944",
                    "line":"zzz"
                }
            ]
        }
    ]
}
```

###### QUERY Labels

```console
# curl localhost:3100/loki/api/v1/label
```

```json
{"status": "success", "data":["__name__"]}
```

###### QUERY Label Values

```console
# curl 'localhost:3100/loki/api/v1/__name__/values'
```

```json
{"status": "success", "data":["up"]}
```
