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
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'cloki' }
};
//clickhouse_options.queryOptions.database = process.env.CLICKHOUSE_DB || 'cloki';

const transpiler = require("../../parser/transpiler");
const rotation_labels  = process.env.LABELS_DAYS || 7;
const rotation_samples = process.env.SAMPLES_DAYS || 7;
const axios = require('axios');

const { StringStream, DataStream} = require("scramjet");

const {parseLabels, hashLabels} = require("../../common");

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
        const  resp = await axios.post(`http://${clickhouse_options.auth}@${clickhouse_options.host}:${clickhouse_options.port}/?query=${this.statement}`,
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
/* TODO: tsv2
const timeSeriesv2Throttler = new TimeoutThrottler(
	`INSERT INTO ${clickhouse_options.queryOptions.database}.time_series_v2(date, fingerprint, labels, name) FORMAT JSONEachRow`);*/
samplesThrottler.start();
timeSeriesThrottler.start();
//timeSeriesv2Throttler.start();

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
	    /* TODO: tsv2
		timeSeriesv2Throttler.queue.push.apply(timeSeriesv2Throttler.queue, value.list.map(r => JSON.stringify({
			date: r.record[0],
			fingerprint: r.record[1],
			labels: JSON.parse(r.record[2]),
			name: r.record[3]
		})));
		*/
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
	var state = 4;
	console.log('Initializing DB...', dbName);
	var dbQuery = "CREATE DATABASE IF NOT EXISTS "+dbName;
	var tmp = { ...clickhouse_options, queryOptions: { database: ''} };
	ch = new ClickHouse(tmp);

	const hack_ch = (ch) => {
		ch._query = ch.query;
		ch.query = (q, opts, cb) => {
			return new Promise(f => ch._query(q, opts, (err, data) => {
				cb(err, data);
				f();
			}));
		}
	}

	ch.query(dbQuery, undefined, async function (err, data) {
		if (err) { console.error('error', err); return; }
		var ch = new ClickHouse(clickhouse_options);
		hack_ch(ch);
		console.log('CREATE TABLES', dbName);

		var ts_table = "CREATE TABLE IF NOT EXISTS "+dbName+".time_series (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint"
		var sm_table = "CREATE TABLE IF NOT EXISTS "+dbName+".samples (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (fingerprint, timestamp_ms)"

	  	await ch.query(ts_table, undefined, function(err,data){
			if (err) { console.log(err); process.exit(1); }
			else if (debug) console.log('Timeseries Table ready!');
			console.log('Timeseries Table ready!');
			return true;
		});
	  	await ch.query(sm_table, undefined, function(err,data){
			if (err) { console.log(err);  process.exit(1); }
			else if (debug) console.log('Samples Table ready!');
			return true;
		});

		var alter_table = "ALTER TABLE "+dbName+".samples MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192";
		var rotate_table = "ALTER TABLE "+dbName+".samples MODIFY TTL toDateTime(timestamp_ms / 1000)  + INTERVAL "+rotation_samples+" DAY";

	  	await ch.query(alter_table, undefined, function(err,data){
			if (err) { console.log(err); }
			else if (debug) console.log('Samples Table altered for rotation!');
			//return true;
		});
	  	await ch.query(rotate_table, undefined, function(err,data){
			if (err){ console.log(err); }
			else if (debug) console.log('Samples Table rotation set to days: '+rotation_samples);
			return true;
		});

		var alter_table = "ALTER TABLE "+dbName+".time_series MODIFY SETTING ttl_only_drop_parts = 1, merge_with_ttl_timeout = 3600, index_granularity = 8192";
		var rotate_table = "ALTER TABLE "+dbName+".time_series MODIFY TTL date  + INTERVAL "+rotation_labels+" DAY";

	  	await ch.query(alter_table, undefined, function(err,data){
			if (err) { console.log(err); }
			else if (debug) console.log('Labels Table altered for rotation!');
			return true;
		});
	  	await ch.query(rotate_table, undefined, function(err,data){
			if (err) { console.log(err); }
			else if (debug) console.log('Labels Table rotation set to days: '+rotation_labels);
			return true;
		});
		/* TODO: tsv2
		const tsv2 = await axios.get(`http://${clickhouse_options.auth}@${clickhouse_options.host}:${clickhouse_options.port}/?query=SHOW TABLES FROM ${dbName} LIKE 'time_series_v2' FORMAT JSON`);
		if (!tsv2.data.rows) {
			const create_tsv2 = `CREATE TABLE IF NOT EXISTS ${dbName}.time_series_v2 
				(
				    date Date,
				    fingerprint UInt64,
					labels Array(Tuple(String, String)),
				    labels_map Map(String, String), 
    				name String
    			) ENGINE = ReplacingMergeTree(date) PARTITION BY date ORDER BY fingerprint`;
			await ch.query(create_tsv2, undefined, () => {});
			const insert = `INSERT INTO ${dbName}.time_series_v2 (date, fingerprint, labels, labels_map, name) 
				SELECT date, fingerprint, JSONExtractKeysAndValues(labels, 'String') as labels, 
				  CAST((
  					arrayMap(x -> x.1, JSONExtractKeysAndValues(labels, 'String')), 
  					arrayMap(x -> x.2, JSONExtractKeysAndValues(labels, 'String'))), 'Map(String, String)') as labels_map,
  					name FROM ${dbName}.time_series`;
			await axios.post(`http://${clickhouse_options.auth}@${clickhouse_options.host}:${clickhouse_options.port}/`,
				insert);
		}*/

		reloadFingerprints();

	});

};
var reloadFingerprints = function(){
  console.log('Reloading Fingerprints...');
  var select_query = `SELECT DISTINCT fingerprint, labels FROM ${clickhouse_options.queryOptions.database}.time_series`;
  var stream = ch.query(select_query);
  // or collect records yourself
	var rows = [];
	stream.on('metadata', function (columns) {
	  // do something with column list
	});
	stream.on('data', function (row) {
	  rows.push (row);
	});
	stream.on('error', function (err) {
	  // TODO: handler error
	});
	stream.on('end', function () {
	  rows.forEach(function(row){
		try {
			var JSON_labels = toJSON(row[1].replace(/\!?=/g,':'));
			labels.add(row[0],JSON.stringify(JSON_labels));
			for (var key in JSON_labels){
				// if (debug) console.log('Adding key',row);
				labels.add('_LABELS_',key);
				labels.add(key,JSON_labels[key]);
			};

		} catch(e) { console.error(e); return }
	  });
	  if (debug) console.log('Reloaded fingerprints:',rows.length+1);
	});

}

var fakeStats = {"summary":{"bytesProcessedPerSecond":0,"linesProcessedPerSecond":0,"totalBytesProcessed":0,"totalLinesProcessed":0,"execTime":0.001301608},"store":{"totalChunksRef":0,"totalChunksDownloaded":0,"chunksDownloadTime":0,"headChunkBytes":0,"headChunkLines":0,"decompressedBytes":0,"decompressedLines":0,"compressedBytes":0,"totalDuplicates":0},"ingester":{"totalReached":1,"totalChunksMatched":0,"totalBatches":0,"totalLinesSent":0,"headChunkBytes":0,"headChunkLines":0,"decompressedBytes":0,"decompressedLines":0,"compressedBytes":0,"totalDuplicates":0}};

var scanFingerprints = async function (query, res) {
	if (debug) console.log('Scanning Fingerprints...');
	const _query = transpiler.transpile(query);
	//console.log(_query.query);
	const _stream = await axios.post(`http://${clickhouse_options.auth}@${clickhouse_options.host}:${clickhouse_options.port}/`,
		_query.query + ' FORMAT JSONEachRow',
		{
			responseType: "stream"
		}
	);
	let lastLabel = null;
	let stream = [];
	let i = 0;
	res.res.writeHead(200, {'Content-Type': "application/json"})
	/**
	 *
	 * @param s {DataStream}
	 */
	let process = (s) => s
	if (!_query.matrix) {
		process = (s) => s.remap((emit, row) => {
			if (lastLabel && !row.labels) {
				emit({
					stream: parseLabels(lastLabel),
					values: stream
				});
			} else if (lastLabel && hashLabels(row.labels) !== hashLabels(lastLabel)) {
				emit({
					stream: parseLabels(lastLabel),
					values: stream
				});
				stream = [];
			}
			lastLabel = row.labels;
			row.timestamp_ms && stream.push([(parseInt(row.timestamp_ms) * 1000000).toString(), row.string]);
		});
	} else {
		const step = UTILS.parseOrDefault(query.step, 5) * 1000;
		const duration = _query.duration;
		let nextTime = 0;
		process = (s) => s.remap((emit, row) => {
			if (lastLabel && (!row.labels || hashLabels(row.labels) !== hashLabels(lastLabel))) {
				if (stream.length === 1) {
					stream.push([stream[0][0] + (step / 1000), stream[0][1]]);
				}
				emit({
					metric: lastLabel ? parseLabels(lastLabel) : {},
					values: stream
				});
				lastLabel = null;
				stream = [];
				nextTime = 0;
			}
			if (!row.labels) {
				return;
			}

			lastLabel = row.labels;
			const timestamp_ms = parseInt(row.timestamp_ms);
			if (timestamp_ms < nextTime) {
				return;
			}
			for (let ts = timestamp_ms; ts < timestamp_ms + duration; ts += step) {
				stream.push([ts / 1000, row.value + ""]);
			}
			nextTime = timestamp_ms + Math.max(duration, step);
		});
	}
	let dStream = StringStream.from(_stream.data).lines().endWith(JSON.stringify({EOF: true}))
		.map(chunk => chunk ? JSON.parse(chunk) : ({}), DataStream)
		.map(chunk => {
			try {
				if (!chunk || !chunk.labels) {
					return chunk;
				}
				const labels = chunk.extra_labels ? {...parseLabels(chunk.labels), ...parseLabels(chunk.extra_labels)} :
					parseLabels(chunk.labels);
				return {...chunk, labels: labels};
			} catch (e) {
				console.log(chunk);
			}
		}, DataStream);
	if (_query.stream && _query.stream.length) {
		_query.stream.forEach(f => {
			dStream = f(dStream)
		});
	}

	const gen = process(dStream).toGenerator();
	res.res.write(`{"status":"success", "data":{ "resultType":"${_query.matrix ? 'matrix' : 'streams'}", "result": [`);
	for await (const item of gen()) {
		if (!item) {
			continue;
		}
		res.res.write((i===0 ? '' : ',') + JSON.stringify(item));
		++i;
	}
	res.res.write(`]}}`);
	res.res.end();


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
	if (!settings.timefield) settings.timefield = process.env.CLICKHOUSE_TIMEFIELD || "record_datetime";

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
			+ " FROM "+settings.db+".samples RIGHT JOIN "+settings.db+".time_series ON samples.fingerprint = time_series.fingerprint"
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
	if (!settings.timefield) settings.timefield = process.env.TIMEFIELD || "record_datetime";

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
module.exports.stop = () => {
    samplesThrottler.stop();
    timeSeriesThrottler.stop();
}
