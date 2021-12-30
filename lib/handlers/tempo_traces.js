/* Tempo Query Handler */
/*
   Returns JSON formatted to /tempo/api/traces API
   Example JSON Response:
   
  {
    "data": [
      {
        "traceID": "d6e9329d67b6146a",
        "spans": [
          {
            "traceID": "d6e9329d67b6146a",
            "spanID": "0000000000001234",
            "operationName": "span from bash!",
            "references": [],
            "startTime": 1640617863672218,
            "duration": 100000,
            "tags": [
              {
                "key": "http.path",
                "type": "string",
                "value": "/api"
              },
              {
                "key": "http.method",
                "type": "string",
                "value": "GET"
              },
              {
                "key": "env",
                "type": "string",
                "value": "prod"
              },
              {
                "key": "status.code",
                "type": "int64",
                "value": 0
              }
            ],
            "logs": [],
            "processID": "p1",
            "warnings": null
          },
          {
            "traceID": "d6e9329d67b6146a",
            "spanID": "0000000000005678",
            "operationName": "child span from bash!",
            "references": [
              {
                "refType": "CHILD_OF",
                "traceID": "d6e9329d67b6146a",
                "spanID": "0000000000001234"
              }
            ],
            "startTime": 1640617865768371,
            "duration": 100000,
            "tags": [
              {
                "key": "env",
                "type": "string",
                "value": "prod"
              },
              {
                "key": "status.code",
                "type": "int64",
                "value": 0
              }
            ],
            "logs": [],
            "processID": "p1",
            "warnings": [
              "clock skew adjustment disabled; not applying calculated delta of -2.096153s"
            ]
          }
        ],
        "processes": {
          "p1": {
            "serviceName": "shell script",
            "tags": []
          }
        },
        "warnings": null
      }
    ],
    "total": 0,
    "limit": 0,
    "offset": 0,
    "errors": null
  }

*/

const protoBuff = require('protobufjs')
const TraceDataType = protoBuff.loadSync(__dirname + '/../opentelemetry/proto/trace/v1/trace.proto')
  .lookupType('opentelemetry.proto.trace.v1.TracesData')


async function handler (req, res) {
  if (this.debug) console.log('GET /api/traces/:traceId')
  if (this.debug) console.log('QUERY: ', req.query)
   if (this.debug) console.log('TRACEID: ',req.params?.traceId)
  const resp = { traces: [] }
  if (!req.params?.traceId) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = `{traceId="${req.params.traceId}"}`
  console.log(req.query.query)
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    /* TODO: Everything to make this look like a tempo traces reply */
    const resp = await this.tempoQueryScan(
      req.query
    )
    // TODO: convert resp to TraceDataType
    // TODO: then res.send(TraceDataType.encode(TraceDataType.fromObject(...)))
  } catch (e) {
    console.log(e)
    res.send(resp)
  }
}

module.exports = handler
