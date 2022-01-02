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



function pad(pad, str, padLeft) {
  if (typeof str === 'undefined') 
    return pad;
  if (padLeft) {
    return (pad + str).slice(-pad.length);
  } else {
    return (str + pad).substring(0, pad.length);
  }
}

async function handler (req, res) {
  if (this.debug) console.log('GET /api/traces/:traceId')
  if (this.debug) console.log('QUERY: ', req.query)
   if (this.debug) console.log('TRACEID: ',req.params?.traceId)
  const resp = { data: [] }
  if (!req.params?.traceId) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = `{traceId="${req.params.traceId}"}`
  console.log('Scan Tempo', req.query, req.params.traceId);
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    /* TODO: Everything to make this look like a tempo traces reply */
    const resp = await this.tempoQueryScan(
      req.query, res, req.params.traceId
    )
    let parsed = JSON.parse(resp);
    console.log('PARSED:', JSON.stringify(parsed));
    if(!parsed.data[0]?.spans) throw new Error('no results');
    let struct = { resourceSpans: [{
            "instrumentationLibrarySpans": [
                {
                    "spans": parsed.data[0].spans
		}
	   ]
    }]};

    struct.resourceSpans = [];
    parsed.data[0].spans.forEach(function(span){
	//span.traceID = Buffer.from(span.traceID, 'utf8').toString('hex')
	//span.spanID = Buffer.from(span.spanID, 'utf8').toString('hex')
	var attributes = [];
	if (span.traceID){
		var tmp = pad('00000000000000000000000000000000',span.traceID,true);
		span.traceId = Buffer.from(tmp, 'hex').toString('base64');
		//attributes.push({ key: "traceID", value: { stringValue: span.traceID }});
	}
	if (span.spanID){
		var tmp = pad('0000000000000000',span.spanID,true);
		span.spanId = Buffer.from(tmp, 'hex').toString('base64');
		//attributes.push({ key: "spanID", value: { stringValue: span.spanID }});
	}
	if (span.parentSpanID){
		var tmp = pad('0000000000000000',span.parentSpanID,true);
		span.parentSpanId = Buffer.from(tmp, 'hex').toString('base64');
		//attributes.push({ key: "spanID", value: { stringValue: span.spanID }});
	}
	if (span.operationName){
		var tmp = pad('00000000000000000000000000000000',span.operationName,true);
		span.operationName = Buffer.from(tmp, 'hex').toString('base64');
		//attributes.push({ key: "spanID", value: { stringValue: span.spanID }});
	}
	// Form a new response object
	var protoJSON = {
		resource: {
        	  attributes: [
	          {
	            key: "service.name",
	            value: {
	              stringValue: "shell script"
	            }
	          }]
	        },
	        instrumentationLibrarySpans: [
		  {
	            instrumentationLibrary: {},
	            spans: [ span ]
	          }
		]
	   };
           protoJSON.resource.attributes = protoJSON.resource.attributes.concat(attributes)
	   struct.resourceSpans.push(protoJSON);
	   console.log('push span', span);
    });

    let inside = TraceDataType.fromObject(struct);
    let proto = TraceDataType.encode(inside).finish();
    //console.log('PB', proto, inside);
    console.log('PB', JSON.stringify(struct));
    console.log('PB-HEX', Buffer.from(proto).toString('hex'));
    res.header('Content-Type', 'application/x-protobuf')
    res.send(proto);
    // TODO: convert resp to TraceDataType
    // TODO: then res.send(TraceDataType.encode(TraceDataType.fromObject(...)))

  } catch (e) {
    console.log(e)
    res.headers({'content-type': 'application/json'}).send(resp)
  }
}

module.exports = handler
