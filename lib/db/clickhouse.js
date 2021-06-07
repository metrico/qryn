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
	auth: process.env.CLICKHOUSE_AUTH || 'default:',
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'default' },
};
const rotation_labels  = process.env.LABELS_DAYS || 7;
const rotation_samples = process.env.SAMPLES_DAYS || 7;
const axios = require('axios');

const clickhouse = new ClickHouse(clickhouse_options);
var ch;

let samples = [];
let timeSeries = [];

class TimeoutThrottler {
    constructor(statement) {
        this.statement = statement;
        this.on = false;
        this.queue = [];
    }
    start() {
        if (this.on) {
            return;
        }
        this.on = true;
        const self = this;
        setTimeout(async () => {
            while (self.on) {
                const ts = Date.now()
                try {
                    await self.flush();
                } catch (e) {

                    if (e.response) {
                        console.log("AXIOS ERROR");
                        console.log(e.message);
                        console.log(e.response.status);
                        console.log(e.response.data);
                    } else {
                        console.log(e)
                    }
                }
                const p = Date.now() - ts;
                if (p < 100) {
                    await new Promise(f => setTimeout(f, 100 - p));
                }
            }
        });
    }
    async flush() {
        const self = this;
        const len = this.queue.length;
        if (len < 1) {
            return;
        }
        const ts = Date.now();
        const  resp = await axios.post(`http://${clickhouse_options.host}:${clickhouse_options.port}/?query=${this.statement}`,
            this.queue.join("\n")
        );
        this.queue = this.queue.slice(len);
    }
    stop() {
        this.on = false;
    }
}

const samplesThrottler = new TimeoutThrottler(
    `INSERT INTO ${clickhouse_options.queryOptions.database}.samples(fingerprint, timestamp_ms, value, string) FORMAT JSONEachRow`);
const timeSeriesThrottler = new TimeoutThrottler(
    `INSERT INTO ${clickhouse_options.queryOptions.database}.time_series(date, fingerprint, labels, name) FORMAT JSONEachRow`);
samplesThrottler.start();
timeSeriesThrottler.start();

/* Cache Helper */
var recordCache = require('record-cache');
var onStale = function(data){
 	for (let [key, value] of data.records.entries()) {
            samplesThrottler.queue.push.apply(samplesThrottler.queue, value.list.map(r => JSON.stringify({
                fingerprint: r.record[0],
                timestamp_ms: r.record[1],
                value: r.record[2],
                string: r.record[3]
            })));
        }

}
var onStale_labels = function(data){
 	for (let [key, value] of data.records.entries()) {
	     timeSeriesThrottler.queue.push.apply(timeSeriesThrottler.queue, value.list.map(r => JSON.stringify({
             date: r.record[0],
             fingerprint: r.record[1],
             labels: r.record[2],
             name: r.record[3]
         })));
        }

}

// Flushing to Clickhouse
var bulk = recordCache({
  maxSize: process.env.BULK_MAXSIZE || 5000,
  maxAge: process.env.BULK_MAXAGE || 2000,
  onStale: onStale
})

var bulk_labels = recordCache({
  maxSize: 100,
  maxAge: 500,
  onStale: onStale_labels
})

// In-Memory LRU for quick lookups
var labels = recordCache({
  maxSize: process.env.BULK_MAXCACHE || 50000,
  maxAge: 0,
  onStale: false
})

/* Initialize */
var initialize = function(dbName){
	console.log('Initializing DB...');
	var dbQuery = "CREATE DATABASE IF NOT EXISTS "+dbName;
	clickhouse.query(dbQuery, function (err, data) {
		if (err) console.error(err);
		if (clickhouse_options.queryOptions.database === dbName){
			ch = clickhouse;
		} else {
			clickhouse_options.queryOptions.database = dbName;
			ch = new ClickHouse(clickhouse_options);
		}
		var ts_table = "CREATE TABLE IF NOT EXISTS "+dbName+".time_series (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint"
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

		var alter_table = "ALTER TABLE "+dbName+".samples MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192";
		var rotate_table = "ALTER TABLE "+dbName+".samples MODIFY TTL toDateTime(timestamp_ms / 1000)  + INTERVAL "+rotation_samples+" DAY";

	  	ch.query(alter_table, function(err,data){
			if (err) console.log(err);
			if (debug) console.log('Samples Table altered for rotation!');
			return true;
		});
	  	ch.query(rotate_table, function(err,data){
			if (err) console.log(err);
			if (debug) console.log('Samples Table rotation set to days: '+rotation_samples);
			return true;
		});

		var alter_table = "ALTER TABLE "+dbName+".time_series MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192";
		var rotate_table = "ALTER TABLE "+dbName+".time_series MODIFY TTL date  + INTERVAL "+rotation_labels+" DAY";

	  	ch.query(alter_table, function(err,data){
			if (err) console.log(err);
			if (debug) console.log('Labels Table altered for rotation!');
			return true;
		});
	  	ch.query(rotate_table, function(err,data){
			if (err) console.log(err);
			if (debug) console.log('Labels Table rotation set to days: '+rotation_labels);
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
			var JSON_labels = toJSON(row[1].replace(/\!?=/g,':'));
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

var fakeStats = {"summary":{"bytesProcessedPerSecond":0,"linesProcessedPerSecond":0,"totalBytesProcessed":0,"totalLinesProcessed":0,"execTime":0.001301608},"store":{"totalChunksRef":0,"totalChunksDownloaded":0,"chunksDownloadTime":0,"headChunkBytes":0,"headChunkLines":0,"decompressedBytes":0,"decompressedLines":0,"compressedBytes":0,"totalDuplicates":0},"ingester":{"totalReached":1,"totalChunksMatched":0,"totalBatches":0,"totalLinesSent":0,"headChunkBytes":0,"headChunkLines":0,"decompressedBytes":0,"decompressedLines":0,"compressedBytes":0,"totalDuplicates":0}};
var scanFingerprints = function(JSON_labels,client,params,label_rules,label_regex,timeseries){
	if (debug) console.log('Scanning Fingerprints...',JSON_labels,label_rules);
	// populate results structure
	var results =
	         {
	            "values": []
	         };

	var resp = {
	   "status":"success",
	   "data":{
	      "resultType":"streams",
	      "result":[results]
	   }
	}


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
	   resp.data.result = [];
	   resp.data.message = err;
	   client.send(resp);
	});
	stream.on ('end', function () {

		if (!finger_rows[0]) { client.send(resp); return; }

		var selectFields = "fingerprint, timestamp_ms, string";
		if (timeseries) {
			resp.data.resultType = 'matrix';
			selectFields = "fingerprint, timestamp_ms, round(value,3)"
		}

	  	if (debug) console.log('FOUND FINGERPRINTS: ', finger_rows);
	  	var select_query = "SELECT "+selectFields
			+ " FROM samples"
			+ " WHERE fingerprint IN ("+finger_rows.join(',')+")"
			if (params.start && params.end) {
				select_query += " AND timestamp_ms BETWEEN "+parseInt(params.start/1000000) +" AND "+parseInt(params.end/1000000)
			}
			if (label_regex) {
				select_query += " AND positionCaseInsensitive(string,'"+label_regex+"')>0";
			}
			if (timeseries) {
			   	// select_query += " AND value > 0";
				select_query += " ORDER BY timestamp_ms"
			} else {
				select_query += " ORDER BY fingerprint, timestamp_ms"
			}
			if (params.limit){
				select_query += " LIMIT "+params.limit*2
			}
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
		  console.log(err);
		  resp.status = "failed";
		  resp.data.result = [];
		  resp.data.message = err;
		  client.send(resp);
		});
		stream.on ('end', function () {
		  if (debug) console.log('RESPONSE:',rows);
		  if (!rows||rows.length < 1) {
			resp.data.result =[];
			resp.data.stats = fakeStats;
		  } else {
			try {
			  rows.forEach(function(row){
				if (timeseries) results.values.push( [ parseInt(row[1] / 1000), row[2].toString() ]);
				else results.values.push( [ parseInt(row[1] * 1000000).toString(), row[2] ]);
		  	  });
			} catch(e){ console.log(e); }
		  }

		  if (timeseries) {
			results.metric = JSON_labels;
		  } else {
			results.stream = JSON_labels;
		  	results.stream.source = "JSON";
		  }
		  //console.log('Final Response',JSON.stringify(resp,null,2));
	  	  client.send(resp);

		});
	});
}

/* cLoki Metrics Column */
var scanMetricFingerprints = function(settings,client,params){

	if (debug) console.log('Scanning Clickhouse...',settings);
	// populate matrix structure
	var resp = {
	   "status":"success",
	   "data":{
	      "resultType":"matrix",
	      "result":[]
	   }
	}
	// Check for required fields or return nothing!
	if(!settings||!settings.table||!settings.db||!settings.tag||!settings.metric) { client.send(resp); return; }
	settings.interval = settings.interval ? parseInt(settings.interval) : 60;
	if (!settings.timefield) settings.timefield = "timestamp_ms";

	var tags = settings.tag.split(',');
	var template =  "SELECT "+tags.join(', ')+", groupArray((toUnixTimestamp(timestamp_ms)*1000, toString(value))) AS groupArr FROM (SELECT ";
			if (tags){
				tags.forEach(function(tag){
					tag = tag.trim();
					template += " visitParamExtractString(labels, '"+tag+"') as "+tag+",";
				})
			}
			//if(settings.interval > 0){
				template += " toStartOfInterval(toDateTime(timestamp_ms/1000), INTERVAL "+settings.interval+" second) as timestamp_ms, value"
			//} else {
			//	template += " timestamp_ms, value"
			//}

			// template += " timestamp_ms, value"
			+ " FROM loki.samples RIGHT JOIN loki.time_series ON samples.fingerprint = time_series.fingerprint"
			if (params.start && params.end) {
			  template += " WHERE "+settings.timefield+" BETWEEN "+parseInt(params.start/1000000000) +" AND "+parseInt(params.end/1000000000)
			  //template += " WHERE "+settings.timefield+" BETWEEN "+parseInt(params.start/1000000) +" AND "+parseInt(params.end/1000000)
			}
			if (tags){
				tags.forEach(function(tag){
					tag = tag.trim();
					template += " AND (visitParamExtractString(labels, '"+tag+"') != '')"
				})
			}
			if (settings.where){
			  template += " AND "+settings.where;
			}
			template += " AND value > 0 ORDER BY timestamp_ms) GROUP BY "+tags.join(', ');

	if (debug) console.log('CLICKHOUSE METRICS SEARCH QUERY',template)

  	var stream = ch.query(template);
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
	  client.code(400).send(err)
	});
	stream.on ('end', function () {
	  if (debug) console.log('CLICKHOUSE RESPONSE:',rows);
	  if (!rows||rows.length < 1) {
		resp.data.result =[];
		resp.data.stats = fakeStats;
	  } else {
		try{
		rows.forEach(function(row){
		    var metrics = { metric: {}, values: [] };
		    var tags = settings.tag.split(',');
		    // bypass empty blocks
		    if(row[row.length-1].length < 1) return;
		    // iterate tags
		    for(i=0; i<row.length -1; i++){
			    metrics.metric[tags[i]] = row[i];
		    }
		    // iterate values
		    row[row.length - 1].forEach(function(row){
			if (row[1] == 0) return;
			metrics.values.push( [ parseInt(row[0]/1000), row[1].toString() ]);
	  	    });
		    resp.data.result.push(metrics);
	  	});
		}catch(e){ console.log(e); }
	  }
	  if (debug) console.log('CLOKI RESPONSE',JSON.stringify(resp));
  	  client.send(resp);
	});
}



/* Clickhouse Metrics Column Query */
var scanClickhouse = function(settings,client,params){
	if (debug) console.log('Scanning Clickhouse...',settings);

	// populate matrix structure
	var resp = {
	   "status":"success",
	   "data":{
	      "resultType":"matrix",
	      "result":[]
	   }
	}

	// Check for required fields or return nothing!
	if(!settings||!settings.table||!settings.db||!settings.tag||!settings.metric) { client.send(resp); return; }
	settings.interval = settings.interval ? parseInt(settings.interval) : 60;
	if (!settings.timefield) settings.timefield = "record_datetime";

	// Lets query!
	var template = 	"SELECT "+settings.tag+", groupArray((t, c)) AS groupArr FROM ("
		     		+ "SELECT (intDiv(toUInt32("+settings.timefield+"), "+settings.interval+") * "+settings.interval+") * 1000 AS t, "+settings.tag+", "+settings.metric+" c "
				+ "FROM "+settings.db+"."+settings.table;
				if (params.start && params.end) {
				template += " PREWHERE "+settings.timefield+" BETWEEN "+parseInt(params.start/1000000000) +" AND "+parseInt(params.end/1000000000)
				}
				if (settings.where){
				template += " AND "+settings.where;
				}
				template += " GROUP BY t, "+settings.tag+" ORDER BY t, "+settings.tag+")";
	template += 	" GROUP BY "+settings.tag+" ORDER BY "+settings.tag
	if (debug) console.log('CLICKHOUSE SEARCH QUERY',template)

  	var stream = ch.query(template);
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
	  client.code(400).send(err)
	});
	stream.on ('end', function () {
	  if (debug) console.log('CLICKHOUSE RESPONSE:',rows);
	  if (!rows||rows.length < 1) {
		resp.data.result =[];
		resp.data.stats = fakeStats;
	  } else {
		try{
		rows.forEach(function(row){
		    var metrics = { metric: {}, values: [] };
		    var tags = settings.tag.split(',');
		    // bypass empty blocks
		    if(row[row.length-1].length < 1) return;
		    // iterate tags
		    for(i=0; i<row.length -1; i++){
			    metrics.metric[tags[i]] = row[i];
		    }
		    // iterate values
		    row[row.length - 1].forEach(function(row){
			if (row[1] == 0) return;
			metrics.values.push( [ parseInt(row[0]/1000), row[1].toString() ]);
	  	    });
		    resp.data.result.push(metrics);
	  	});
		}catch(e){ console.log(e); }
	  }
	  if (debug) console.log('CLOKI RESPONSE',JSON.stringify(resp));
  	  client.send(resp);
	});
}


/* Module Exports */

module.exports.database_options = clickhouse_options;
module.exports.database = clickhouse;
module.exports.cache = { bulk: bulk, bulk_labels: bulk_labels, labels: labels };
module.exports.scanFingerprints = scanFingerprints;
module.exports.scanMetricFingerprints = scanMetricFingerprints;
module.exports.scanClickhouse = scanClickhouse;
module.exports.reloadFingerprints = reloadFingerprints;
module.exports.init = initialize;
