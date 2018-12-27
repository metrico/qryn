/*
 * Loki API to Clickhouse Gateway
 * (C) 2018-2019 QXIP BV
 * Some Rights Reserved.
 */

/* TODO: split into modules and prioritize performance! contributors help yourselves :) */

var debug = process.env.DEBUG || false;

/* DB Helper */
const ClickHouse = require('@apla/clickhouse');
const clickhouse_options = {
    host: process.env.CLICKHOUSE_SERVER || 'localhost',
    port: process.env.CLICKHOUSE_PORT || 8123,
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'default' }
};
var clickhouse = new ClickHouse(clickhouse_options);
var ch;

/* ProtoBuf Helper */
var fs = require('fs');
var protoBuff = require("protocol-buffers");
var messages = protoBuff(fs.readFileSync('lib/loki.proto'))

/* Fingerprinting */
var shortHash = require("short-hash")
var fingerPrint = function(text,hex){
	if (hex) return shortHash(text);
	else return parseInt(shortHash(text), 16);
}
const toJSON = require('jsonic');

/* Cache Helper */
var recordCache = require('record-cache');
var onStale = function(data){
 	for (let [key, value] of data.records.entries()) {
	     var statement = "INSERT INTO samples(fingerprint, timestamp_ms, value, string, name)";
	     var clickStream = ch.query (statement, {inputFormat: 'TSV'}, function (err) {
	       if (err) console.log('ERROR BULK',err);
	       if (debug) console.log ('Insert Samples complete for',key);
	     });
 	     value.list.forEach(function(row){
		if (!row.record) return;
		clickStream.write ( row.record );
             });
	     clickStream.end ();
        }
}
var onStale_labels = function(data){
 	for (let [key, value] of data.records.entries()) {
	     var statement = "INSERT INTO time_series(date, fingerprint, labels)";
	     var clickStream = ch.query (statement, {inputFormat: 'TSV'}, function (err) {
	       if (err) console.log('ERROR BULK',err);
	       if (debug) console.log ('Insert Labels complete for',key);
	     });
 	     value.list.forEach(function(row){
		if (!row.record) return;
		clickStream.write ( row.record );
             });
	     clickStream.end ();
        }
}

// Flushing to Clickhouse
var bulk = recordCache({
  maxSize: 5000,
  maxAge: 2000,
  onStale: onStale
})
var bulk_labels = recordCache({
  maxSize: 100,
  maxAge: 500,
  onStale: onStale_labels
})

// In-Memory LRU for quick lookups
var labels = recordCache({
  maxSize: 50000,
  maxAge: 0,
  onStale: false
})

/* Function Helpers */
var databaseName;
var initialize = function(dbName){
	console.log('Initializing DB...');
	var dbQuery = "CREATE DATABASE IF NOT EXISTS "+dbName;
	clickhouse.query(dbQuery, function (err, data) {
		if (err) return err;
		databaseName = dbName;
		clickhouse_options.queryOptions.database = dbName;
		ch = new ClickHouse(clickhouse_options);
		var ts_table = "CREATE TABLE IF NOT EXISTS "+dbName+".time_series (date Date,fingerprint UInt64,labels String) ENGINE = ReplacingMergeTree PARTITION BY date ORDER BY fingerprint"
		var sm_table = "CREATE TABLE IF NOT EXISTS "+dbName+".samples (fingerprint UInt64,timestamp_ms Int64,value Float64,string String, name String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (fingerprint, timestamp_ms)"

	  	ch.query(ts_table, function(err,data){
			if (err) return err;
			if (debug) console.log('Timeseries Table ready!');
			return true;
		});
	  	ch.query(sm_table, function(err,data){
			if (err) return err;
			if (debug) console.log('Samples Table ready!');
			return true;
		});
		reloadFingerprints();
	});
};
var reloadFingerprints = function(){
  console.log('Reloading Fingerprints...');
  var select_query = "SELECT DISTINCT fingerprint, labels FROM time_series";
  var stream = ch.query(select_query);
  // or collect records yourself
	var rows = [];
	stream.on ('metadata', function (columns) {
	  // do something with column list
	});
	stream.on ('data', function (row) {
	  rows.push (row);
	});
	stream.on ('error', function (err) {
	  // TODO: handler error
	});
	stream.on ('end', function () {
	  rows.forEach(function(row){
		var JSON_labels = toJSON(row[1].replace('=',':'));
		labels.add(row[0],JSON.stringify(JSON_labels));
		for (var key in JSON_labels){
			if (debug) console.log('Adding key',row);
			labels.add('_LABELS_',key);
			labels.add(key,JSON_labels[key]);
		};
	  });
	  if (debug) console.log('Reloaded fingerprints:',rows.length+1);
	});

}
var scanFingerprints = function(JSON_labels,client){
	if (debug) console.log('Scanning Fingerprints...',JSON_labels);
	var resp = { "streams": [] };
	var conditions = [];
	for (var key in JSON_labels){
		conditions.push("labels like '%" +key+ "%" +JSON_labels[key] +"%'");
  	}
  	var finger_search = "select DISTINCT fingerprint from time_series where "+conditions.join(' OR ');
  	if (debug) console.log('QUERY',finger_search);

  	var stream = ch.query(finger_search);
	var finger_rows = [];
	stream.on ('data', function (row) {
	   finger_rows.push(row[0]);
	});
	stream.on ('error', function (err) {
	   // client.send(resp);
	});
	stream.on ('end', function () {

		if (!finger_rows[0]) { client.send(resp); return; }

	  	if (debug) console.log('FOUND FINGERPRINTS: ', finger_rows);
	  	var select_query = "SELECT fingerprint, timestamp_ms, string"
			+ " FROM samples"
			+ " WHERE fingerprint IN ("+finger_rows.join(',')+")"
			+ " ORDER BY fingerprint, timestamp_ms"
	  	var stream = ch.query(select_query);
	  	// or collect records yourself
		var rows = [];
			stream.on ('metadata', function (columns) {
		  // do something with column list
		});
		stream.on ('data', function (row) {
		  rows.push (row);
		});
		stream.on ('error', function (err) {
		  // TODO: handler error
		});
		stream.on ('end', function () {
		  if (debug) console.log('RESPONSE:',rows);
		  var entries = [];
		  rows.forEach(function(row){
			entries.push({ "timestamp": row[1], "line": row[2] })
		  });
	  	  resp.streams.push( { "labels": JSON.stringify(JSON_labels), "entries": entries }  );

	  	  client.send(resp);

		});
	});
}

initialize(process.env.CLICKHOUSE_TSDB || 'loki');

/* Fastify Helper */

const fastify = require('fastify')({
  logger: false
})

fastify.register(require('fastify-url-data'), (err) => {
  if (err) throw err
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
  var streams;
  if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/json') > -1) {
	streams = req.body.streams;
  } else if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/protobuf') > -1) {
	streams = messages.PushRequest.decode(req.body)
  }
  if (streams) {
	streams.forEach(function(stream){
		try {
			var JSON_labels = toJSON(stream.labels.replace('=',':'));
			// Calculate Fingerprint
			var finger = fingerPrint(JSON.stringify(JSON_labels));
			if (debug) console.log('LABELS FINGERPRINT',stream.labels,finger);
			labels.add(finger,stream.labels);
			// Store Fingerprint
 			bulk_labels.add(finger,[new Date().toISOString().split('T')[0], finger, JSON.stringify(JSON_labels));
			for(var key in JSON_labels) {
			   if (debug) console.log('Storing label',key, JSON_labels[key]);
			   labels.add('_LABELS_',key); labels.add(key, JSON_labels[key]);
			}
		} catch(e) { console.log(e) }

		if (stream.entries) {
			stream.entries.forEach(function(entry){
				var values = [ finger, new Date(entry.timestamp).getTime(), entry.value || 0, entry.line || "", entry['__name__'] || '' ];
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

  var JSON_labels = toJSON(req.query.query.replace('=',':'));
  scanFingerprints(JSON_labels,res);

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
