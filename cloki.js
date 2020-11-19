/*
 * Loki API to Clickhouse Gateway
 * (C) 2018-2019 QXIP BV
 */

/* TODO: split into modules and prioritize performance! contributors help yourselves :) */

var debug = process.env.DEBUG || false;
var http_user = process.env.CLOKI_LOGIN || false;
var http_pass = process.env.CLOKI_PASSWORD || false;


var DATABASE = require('./lib/db/clickhouse');
var UTILS = require('./lib/utils');

/* ProtoBuf Helper */
var fs = require('fs');
var protoBuff = require("protocol-buffers");
var messages = protoBuff(fs.readFileSync('lib/loki.proto'));

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
var scanClickhouse = DATABASE.scanClickhouse;

init(process.env.CLICKHOUSE_TSDB || 'loki');


/* Fastify Helper */
const fastify = require('fastify')({
  logger: false
})

const path = require('path')
fastify.register(require('fastify-static'), {
  root: path.join(__dirname, 'web'),
  prefix: '/', // optional: default '/'
})

fastify.register(require('fastify-url-data'), (err) => {
  if (err) throw err
})

/* Enable Simple Authentication */
if (http_user && http_password){
  fastify.register(require('fastify-basic-auth'), { validate })
  fastify.after(() => {
    fastify.addHook('preHandler', fastify.basicAuth)
  })
}

function validate (username, password, req, reply, done) {
    if (username === http_user && password === http_password) {
        done()
    } else {
        done(new Error('Unauthorized!: Wrong username/password.'))
    }
}

fastify.addContentTypeParser('application/x-protobuf', function (req, done) {
  var data = ''
  req.on('data', chunk => { data += chunk })
  req.on('error', (error) => { console.log(error) })
  req.on('end', () => {
    done(messages.PushRequest.decode(data))
  })
})


fastify.get('/hello', (request, reply) => {
  reply.send({ hello: 'cloki' })
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

fastify.post('/loki/api/v1/push', (req, res) => {
  if (debug) console.log('POST /loki/api/v1/push');
  if (debug) console.log('QUERY: ', req.query);
  if (debug) console.log('BODY: ', req.body);
  if (!req.body) {
	 console.error('No Request Body!', req);
	 return;
  }
  var streams;
  if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/json') > -1) {
	streams = req.body.streams;
  } else if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/x-protobuf') > -1) {
	// streams = messages.PushRequest.decode(req.body)
	streams = req.body;
	if (debug) console.log('GOT protoBuf',streams);
  }
  if (streams) {
	streams.forEach(function(stream){
		try {
			try {
				var JSON_labels = toJSON(stream.labels.replace(/\!?="/g,':"'));
			} catch(e) { console.error(e); return; }
			// Calculate Fingerprint
			var finger = fingerPrint(JSON.stringify(JSON_labels));
			if (debug) console.log('LABELS FINGERPRINT',stream.labels,finger);
			labels.add(finger,stream.labels);
			// Store Fingerprint
 			bulk_labels.add(finger,[new Date().toISOString().split('T')[0], finger, JSON.stringify(JSON_labels), JSON_labels['name']||'' ]);
			for(var key in JSON_labels) {
			   if (debug) console.log('Storing label',key, JSON_labels[key]);
			   labels.add('_LABELS_',key); labels.add(key, JSON_labels[key]);
			}
		} catch(e) { console.log(e) }

		if (stream.entries) {
			stream.entries.forEach(function(entry){
				if (debug) console.log('BULK ROW',entry,finger);
				if ( !entry && (!entry.timestamp||!entry.ts) && (!entry.value||!entry.line)) { console.error('no bulkable data',entry); return; }
				var values = [ finger, new Date(entry.timestamp||entry.ts).getTime(), entry.value || 0, entry.line || "" ];
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

fastify.get('/loki/api/v1/query_range', (req, res) => {
  if (debug) console.log('GET /loki/api/v1/query_range');
  if (debug) console.log('QUERY: ', req.query );
  // console.log( req.urlData().query.replace('query=',' ') );
  var params = req.query;
  var resp = { "streams": [] };
  if (!req.query.query) { res.send(resp); return; }

  /* query templates */
  var RATEQUERY = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*)/;
  var RATEQUERYWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*) where (.*)/;
  var RATEQUERYNOWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.([\S]+)\s?(.*)/;


  if (!req.query.query) {
	res.code(400).send('invalid query');

  } else if (RATEQUERYNOWHERE.test(req.query.query)){
	var s = RATEQUERYNOWHERE.exec(req.query.query);
	console.log('tags',s);
	var JSON_labels = { db: s[5], table: s[6], interval: s[4] || 60, tag: s[2], metric: s[1]+'('+s[3]+')', where: s[3]+" "+s[7] };
	scanClickhouse(JSON_labels,res,params);

  } else if (RATEQUERYWHERE.test(req.query.query)){
	var s = RATEQUERYWHERE.exec(req.query.query);
	console.log('tags',s);
	var JSON_labels = { db: s[5], table: s[6], interval: s[4] || 60, tag: s[2], metric: s[1]+'('+s[3]+')', where: s[7] };
	scanClickhouse(JSON_labels,res,params);

  } else if (RATEQUERY.test(req.query.query)){
	var s = RATEQUERY.exec(req.query.query);
	console.log('tags',s);
	var JSON_labels = { db: s[5], table: s[6], interval: s[4] || 60, tag: s[2], metric: s[1]+'('+s[3]+')' };
	scanClickhouse(JSON_labels,res,params);

  } else if (req.query.query.startsWith("clickhouse(")){

     try {
	  var query = /\{(.*?)\}/g.exec(req.query.query)[1] || req.query.query;
	  var queries = query.replace(/\!?="/g,':"');
	  var JSON_labels = toJSON(queries);
     } catch(e){ console.error(e, queries); res.send(resp); }
     if (debug) console.log('SCAN CLICKHOUSE',JSON_labels,params)
     scanClickhouse(JSON_labels,res,params);
  } else {
     try {
	  var label_parser = labelParser(req.query.query);
	  var label_rules = label_parser.labels;
	  var label_regex = label_parser.regex;
	  var query = /\{(.*?)\}/g.exec(req.query.query)[1] || req.query.query;
	  var queries = query.replace(/\!?="/g,':"');
	  var JSON_labels = toJSON(queries);
     } catch(e){ console.error(e, queries); res.send(resp); }
     if (debug) console.log('SCAN LABELS',JSON_labels,label_rules,params)
     scanFingerprints(JSON_labels,res,params,label_rules,label_regex);

  }

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

/* Label Value Handler via query (test) */
fastify.get('/loki/api/v1/query', (req, res) => {
  if (debug) console.log('GET /loki/api/v1/query');
  if (debug) console.log('QUERY: ', req.query );
  var query = req.query.query.replace(/\!?="/g,':"');

  // console.log( req.urlData().query.replace('query=',' ') );
  var all_values = labels.get(query.name);
  if (!all_values || all_values.length == 0) {
	var resp = {"status":"success","data":{"resultType":"streams","result":[]}};
  } else {
  	var resp = { "values": all_values };
  }
  if (debug) console.log('LABEL',query.name,'VALUES', all_values);
  res.send(resp);
});

fastify.get('/loki/api/v1/label', (req, res) => {
  if (debug) console.log('GET /loki/api/v1/label');
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

fastify.get('/loki/api/v1/label/:name/values', (req, res) => {
  if (debug) console.log('GET /api/prom/label/'+req.params.name+'/values');
  if (debug) console.log('QUERY LABEL: ', req.params.name);
  var all_values = labels.get(req.params.name);
  var resp = { "values": all_values };
  res.send(resp);
});

/* Series Placeholder - we do not track this as of yet */
fastify.get('/loki/api/v1/series', (req, res) => {
  if (debug) console.log('GET /api/v1/series/'+req.params.name+'/values');
  if (debug) console.log('QUERY SERIES: ', req.params);
  var resp = { "status": "success", "data": []};
  res.send(resp);
});

// Run API Service
fastify.listen(process.env.PORT || 3100, process.env.HOST || '0.0.0.0', (err, address) => {
  if (err) throw err
  console.log('cLoki API up');
  fastify.log.info(`server listening on ${address}`)
})
