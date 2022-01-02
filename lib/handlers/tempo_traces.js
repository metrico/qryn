/* cLoki Tempo Query Handler */
/*
   Returns Protobuf-JSON formatted to /tempo/api/traces API
   Protobuf JSON Schema: https://github.com/lmangani/cLoki/pull/87#issuecomment-1003616559
   API Push Example: https://github.com/lmangani/cLoki/pull/87#issuecomment-1002683058
   
   TODO: 
   - Refactor code and optimize interfacing with db/clickhouse.js and handler/tempo_push.js
   - Optimize for performance and reduce/remove POC debug layers
   
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
  if (this.debug) console.log('Scan Tempo', req.query, req.params.traceId);
  /* scan fingerprints */
  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    const resp = await this.tempoQueryScan(
      req.query, res, req.params.traceId
    )
    let parsed = JSON.parse(resp);
    if (this.debug) console.log('PARSED:', JSON.stringify(parsed));
    if(!parsed.data[0]?.spans) throw new Error('no results');
    /* Basic Structure for traces/v1 Protobuf encoder */
    let struct = { resourceSpans: [] }]};
	
    /* Reformat data from collected spans (includes overkill steps) */
    parsed.data[0].spans.forEach(function(span){
	var attributes = [];
	if (span.serviceName){
		attributes.push({ key: "service.name", value: { stringValue: span.serviceName }});
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
	if (span.operationName){
		var tmp = pad('00000000000000000000000000000000',span.operationName,true);
		span.operationName = Buffer.from(tmp, 'hex').toString('base64');
		//attributes.push({ key: "operationName", value: { stringValue: span.operationName }});
	}
	/* Form a new span/v1 Protobuf-JSON response object wrapper */
	var protoJSON = {
		resource: {
        	  attributes: [
	          {
	            key: "collector",
	            value: {
	              stringValue: "cloki"
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
	if (this.debug) console.log('push span', span);
    });
    /* Pack spans into Protobuf response */
    let inside = TraceDataType.fromObject(struct);
    let proto = TraceDataType.encode(inside).finish();
    if (this.debug) console.log('PB-JSON', JSON.stringify(struct));
    if (this.debug) console.log('PB-HEX', Buffer.from(proto).toString('hex'));
    res.header('Content-Type', 'application/x-protobuf')
    res.send(proto);

  } catch (e) {
    console.log(e)
    res.headers({'content-type': 'application/json'}).send(resp)
  }
}

module.exports = handler
