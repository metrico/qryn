/*
 * Loki API to Clickhouse Gateway
 * (C) 2018-2019 QXIP BV
 * Some Rights Reserved.
 */

/* TODO: split into modules and prioritize performance! contributors help yourselves :) */

var debug = process.env.DEBUG || false;

var DATABASE = require('./lib/db/clickhouse');
var UTILS = require('./lib/utils');

/* ProtoBuf Helper */
var fs = require('fs');
var protoBuff = require("protocol-buffers");
var messages = protoBuff(fs.readFileSync('lib/loki.proto'))

/* Fingerprinting */
var fingerPrint = UTILS.fingerPrint;
var toJSON = UTILS.toJSON;

// Database Bulk Helpers */
var bulk = DATABASE.cache.bulk; // samples
var bulk_labels = DATABASE.cache.bulk_labels; // labels
var labels = DATABASE.cache.labels; // in-memory labels

/* Function Helpers */
var labelParser = UTILS.labelParser;

var init = DATABASE.init;
var reloadFingerprints = DATABASE.reloadFingerprints;
var scanFingerprints = DATABASE.scanFingerprints;

init(process.env.CLICKHOUSE_TSDB || 'loki');


/* Fastify Helper */
const fastify = require('fastify')({
  logger: false
})

fastify.register(require('fastify-url-data'), (err) => {
  if (err) throw err
})

fastify.addContentTypeParser('application/x-protobuf', function (req, done) {
    done()
})

fastify.addContentTypeParser('*', function (req, done) {
  done()
})

fastify.get('/', (request, reply) => {
  reply.send({ hello: 'loki' })
})


/* Write Handler */
/*
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.
    Example of the JSON format:

	{
	    "streams": [
	        {
	            "labels": "{foo=\"bar\"}",
	            "entries": [
	                {"ts": "2018-12-18T08:28:06.801064-04:00", "line": "baz"}
	            ]
	        }
	    ]
	}
*/

fastify.post('/api/prom/push', (req, res) => {
  if (debug) console.log('POST /api/prom/push');
  if (debug) console.log('QUERY: ', req.query);
  if (debug) console.log('BODY: ', req.body);
  if (!req.body) return;
  var streams;
  if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/json') > -1) {
	streams = req.body.streams;
  } else if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/x-protobuf') > -1) {
	streams = messages.PushRequest.decode(req.body)
	if (debug) console.log('protoBuf',streams);
  }
  if (streams) {
	streams.forEach(function(stream){
		try {
			try {
				var JSON_labels = toJSON(stream.labels.replace(/\!?=/g,':'));
			} catch(e) { console.error(e); return; }
			// Calculate Fingerprint
			var finger = fingerPrint(JSON.stringify(JSON_labels));
			if (debug) console.log('LABELS FINGERPRINT',stream.labels,finger);
			labels.add(finger,stream.labels);
			// Store Fingerprint
 			bulk_labels.add(finger,[new Date().toISOString().split('T')[0], finger, JSON.stringify(JSON_labels), JSON_labels['__name__']||'' ]);
			for(var key in JSON_labels) {
			   if (debug) console.log('Storing label',key, JSON_labels[key]);
			   labels.add('_LABELS_',key); labels.add(key, JSON_labels[key]);
			}
		} catch(e) { console.log(e) }

		if (stream.entries) {
			stream.entries.forEach(function(entry){
				var values = [ finger, new Date(entry.timestamp).getTime(), entry.value || 0, entry.line || "" ];
				bulk.add(finger,values);
			})
		}
	});
  }
  res.send(200);
});


/* Query Handler */
/*
   For doing queries, accepts the following parameters in the query-string:

	query: a logQL query
	limit: max number of entries to return
	start: the start time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
	end: the end time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
	direction: forward or backward, useful when specifying a limit
	regexp: a regex to filter the returned results, will eventually be rolled into the query language
*/

fastify.get('/api/prom/query', (req, res) => {
  if (debug) console.log('GET /api/prom/query');
  if (debug) console.log('QUERY: ', req.query );
  // console.log( req.urlData().query.replace('query=',' ') );
  var params = req.query;
  var resp = { "streams": [] };
  if (!req.query.query) { res.send(resp);return; }
  try {

	  var label_rules = labelParser(req.query.query);
	  var queries = req.query.query.replace(/\!?=/g,':');
	  var JSON_labels = toJSON(queries);
  } catch(e){ console.error(e, queries); res.send(resp); }

  scanFingerprints(JSON_labels,res,params,label_rules);

});


/* Label Handlers */
/*
   For retrieving the names of the labels one can query on.
   Responses looks like this:

	{
	  "values": [
	    "instance",
	    "job",
	    ...
	  ]
	}
*/

fastify.get('/api/prom/label', (req, res) => {
  if (debug) console.log('GET /api/prom/label');
  if (debug) console.log('QUERY: ', req.query);
  var all_labels = labels.get('_LABELS_');
  var resp = { "values": all_labels };
  res.send(resp);
});

/* Label Value Handler */
/*
   For retrieving the label values one can query on.
   Responses looks like this:

	{
	  "values": [
	    "default",
	    "cortex-ops",
	    ...
	  ]
	}
*/

fastify.get('/api/prom/label/:name/values', (req, res) => {
  if (debug) console.log('GET /api/prom/label/'+req.params.name+'/values');
  if (debug) console.log('QUERY LABEL: ', req.params.name);
  var all_values = labels.get(req.params.name);
  var resp = { "values": all_values };
  res.send(resp);
});

// Run API Service
fastify.listen(process.env.PORT || 3100, process.env.HOST || '0.0.0.0', (err, address) => {
  if (err) throw err
  console.log('cLoki API up');
  fastify.log.info(`server listening on ${address}`)
})
