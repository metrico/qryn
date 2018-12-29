/*
 * cLoki DB Adapter for Clickhouse
 * (C) 2018-2019 QXIP BV
 */

var debug = process.env.DEBUG || false;
var UTILS = require('../utils');
var toJSON = UTILS.toJSON;

/* DB Helper */
const ClickHouse = require('@apla/clickhouse');
const clickhouse_options = {
    host: process.env.CLICKHOUSE_SERVER || 'localhost',
    port: process.env.CLICKHOUSE_PORT || 8123,
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'default' }
};
var clickhouse = new ClickHouse(clickhouse_options);

/* Cache Helper */
var recordCache = require('record-cache');
var onStale = function(data){
 	for (let [key, value] of data.records.entries()) {
	     var statement = "INSERT INTO samples(fingerprint, timestamp_ms, value, string)";
   	     ch = new ClickHouse(clickhouse_options);
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
	     var statement = "INSERT INTO time_series(date, fingerprint, labels, name)";
   	     ch = new ClickHouse(clickhouse_options);
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

/* Initialize */
var databaseName;
var initialize = function(dbName){
	console.log('Initializing DB...');
	var dbQuery = "CREATE DATABASE IF NOT EXISTS "+dbName;
	clickhouse.query(dbQuery, function (err, data) {
		if (err) console.error(err);
		databaseName = dbName;
		clickhouse_options.queryOptions.database = dbName;
		ch = new ClickHouse(clickhouse_options);
		var ts_table = "CREATE TABLE IF NOT EXISTS "+dbName+".time_series (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree PARTITION BY date ORDER BY fingerprint"
		var sm_table = "CREATE TABLE IF NOT EXISTS "+dbName+".samples (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (fingerprint, timestamp_ms)"

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
		try {
			var JSON_labels = toJSON(row[1].replace(/=/g,':'));
			labels.add(row[0],JSON.stringify(JSON_labels));
			for (var key in JSON_labels){
				if (debug) console.log('Adding key',row);
				labels.add('_LABELS_',key);
				labels.add(key,JSON_labels[key]);
			};

		} catch(e) { console.error(e); return }
	  });
	  if (debug) console.log('Reloaded fingerprints:',rows.length+1);
	});

}
var scanFingerprints = function(JSON_labels,client,params,label_rules){
	if (debug) console.log('Scanning Fingerprints...',JSON_labels);
	var resp = { "streams": [] };
	if (!JSON_labels) return resp;
	var conditions = [];
	// for (var key in JSON_labels){
	//	conditions.push("labels like '%" +key+ "%" +JSON_labels[key] +"%'");
  	// }
	if (debug) console.log('Parsing Rules...',label_rules);
	label_rules.forEach(function(rule){
		if (debug) console.log('Parsing Rule...',rule);
		if (rule[1] == '='){
			conditions.push("(visitParamExtractString(labels, '"+rule[0]+"') = '"+rule[2]+"')")
		} else if (rule[1] == '!='){
			conditions.push("(visitParamExtractString(labels, '"+rule[0]+"') != '"+rule[2]+"')")
		}
	});

	var finger_search = "SELECT DISTINCT fingerprint FROM time_series FINAL PREWHERE "+conditions.join('OR');
  	if (debug) console.log('FINGERPRINT QUERY',finger_search);

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
			if (params.start && params.end) {
				select_query += " AND timestamp_ms BETWEEN "+params.start/1000000 +" AND "+params.end/1000000
			}
			select_query += " ORDER BY fingerprint, timestamp_ms"
		if (debug) console.log('SEARCH QUERY',select_query)
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
			entries.push({ "timestamp": new Date(parseInt(row[1])).toISOString(), "line": row[2] })
		  });
	  	  resp.streams.push( { "labels": JSON.stringify(JSON_labels).replace(/:/g,'='), "entries": entries }  );

	  	  client.send(resp);

		});
	});
}


/* Module Exports */

module.exports.database_options = clickhouse_options;
module.exports.database = clickhouse;
module.exports.cache = { bulk_samples: bulk, bulk_labels: bulk_labels, labels: labels };
module.exports.scanFingerprints = scanFingerprints;
module.exports.reloadFingerprints = reloadFingerprints;
module.exports.init = initialize;
