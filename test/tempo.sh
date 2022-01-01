#!/bin/bash

curl -X POST http://localhost:3100/tempo/api/push -H 'Content-Type: application/json' -d '[{
 "id": "1234",
 "traceId": "d6e9329d67b6146a",
 "timestamp": '$(date +%s%N | cut -b1-16)',
 "duration": 100000,
 "name": "span from bash!",
 "tags": {
    "http.method": "GET",
    "http.path": "/api"
  },
  "localEndpoint": {
    "serviceName": "shell script"
  }
}]'

curl -X POST http://localhost:3100/tempo/api/push -H 'Content-Type: application/json' -d '[{
 "id": "5678",
 "traceId": "d6e9329d67b6146a",
 "parentId": "1234",
 "timestamp": '$(date +%s%N | cut -b1-16)',
 "duration": 100000,
 "name": "child span from bash!",
  "localEndpoint": {
    "serviceName": "shell script"
  }
}]'

sleep 2
curl http://localhost:3100/api/traces/d6e9329d67b6146a
