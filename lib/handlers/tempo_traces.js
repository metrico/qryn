/* Qryn Tempo Query Handler */
/*
   Returns Protobuf-JSON formatted to /tempo/api/traces API
   Protobuf JSON Schema: https://github.com/metrico/qryn/pull/87#issuecomment-1003616559
   API Push Example: https://github.com/metrico/qryn/pull/87#issuecomment-1002683058

   TODO:
   - Refactor code and optimize interfacing with db/clickhouse.js and handler/tempo_push.js
   - Optimize for performance and reduce/remove POC debug layers

*/

const protoBuff = require('protobufjs')
const TraceDataType = protoBuff.loadSync(__dirname + '/../opentelemetry/proto/trace/v1/trace.proto')
  .lookupType('opentelemetry.proto.trace.v1.TracesData')

const logger = require('../logger')

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
  req.log.debug('GET /api/traces/:traceId/:json')
  const json_api = req.params.json || false;
  const resp = { data: [] }
  if (!req.params.traceId) {
    res.send(resp)
    return
  }
  /* remove newlines */
  if (this.tempo_tagtrace) req.query.query = `{traceId="${req.params.traceId}"}`
  else req.query.query = `{type="tempo"} |~ "${req.params.traceId}"`

  req.log.debug('Scan Tempo', req.query, req.params.traceId);
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    const resp = await this.tempoQueryScan(
      req.query, res, req.params.traceId
    )
    let parsed = JSON.parse(resp);
    req.log.debug({ parsed }, 'PARSED');
    if(!parsed.data[0] || !parsed.data[0].spans) throw new Error('no results');
    /* Basic Structure for traces/v1 Protobuf encoder */
    let struct = { resourceSpans: [] };

    /* Reformat data from collected spans (includes overkill steps) */
    parsed.data[0].spans.forEach(function(span){
	var attributes = [];
	if (span.localEndpoint.serviceName || span.serviceName){
		attributes.push({ key: "service.name", value: { stringValue: span.localEndpoint.serviceName || span.serviceName }});
	}
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
		//attributes.push({ key: "parentSpanID", value: { stringValue: span.parentSpanID }});
	}
	if (span.operationName && !span.name){
		span.name = span.operationName;
		//attributes.push({ key: "operation.name", value: { stringValue: span.operationName }});
	}
	if (span.tags.length > 0){
	   /* Temp: Merge Zipkin Tags to Attributes */
           span.tags.forEach(function(tag){
                attributes.push({ key: tag.key, value: { stringValue: tag.value || '' }});
           })
        }   
	/* Form a new span/v1 Protobuf-JSON response object wrapper */
	var protoJSON = {
		resource: {
        	  attributes: [
	          {
	            key: "collector",
	            value: {
	              stringValue: "qryn"
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
	/* Merge Attributes */
        if (attributes.length > 0) protoJSON.resource.attributes = protoJSON.resource.attributes.concat(attributes)
	/* Add to Protobuf-JSON struct */
	struct.resourceSpans.push(protoJSON);
	req.log.debug({ span }, 'push span');
    });

    if(json_api){
	    /* Send spans into JSON response */
	    req.log.debug({ struct }, 'PB-JSON');
    	    res.headers({'content-type': 'application/json'}).send(struct)
    } else {
	    /* Pack spans into Protobuf response */
	    let inside = TraceDataType.fromObject(struct);
	    let proto = TraceDataType.encode(inside).finish();
	    req.log.debug({ struct }, 'PB-JSON');
	    req.log.debug({ proto: Buffer.from(proto).toString('hex') }, 'PB-HEX');
	    res.header('Content-Type', 'application/x-protobuf')
	    res.send(proto);
    }

  } catch (err) {
    req.log.error({ err })
    res.headers({'content-type': 'application/json'}).send(resp)
  }
}

module.exports = handler
