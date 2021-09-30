/* Function Helpers */

/* Label Parser */
var labelParser = function(labels){
	// Label Parser
	var rx = /\"?\b(\w+)\"?(!?=~?)("[^"\n]*?")/g;
	var matches, output = [];
	while (matches = rx.exec(labels)) {
	    if(matches.length >3) output.push([matches[1],matches[2],matches[3].replace(/['"]+/g, '')]);
	}
	try {
	        var regex = /\}\s*(.*)/g.exec(labels)[1] || false;
	} catch(e) { var regex = false; }
	return { labels: output, regex: regex};
}
/* Fingerprinting */
var shortHash = require("short-hash")
var fingerPrint = function(text,hex){
        if (hex) return shortHash(text);
        else return parseInt(shortHash(text), 16);
}

const toJSON = require('jsonic');

/* clickhouse query parser */
var clickParser = function(query){
   /* Example cQL format */
   /* clickhouse({db="mydb", table="mytable", tag="key", metric="avg(value)", interval=60}) */
   var regx = /clickhouse\((.*)\)/g
   var clickQuery = regx.exec(req.query.query)[1] || false;
   return labelParser(clickQuery);
}

const parseOrDefault = (str, def) => {
    try {
        return str ? parseFloat(str) : def;
    } catch (e) {
        return def;
    }
}

const parseMs = (time, def) => {
    try {
        return time ? Math.floor(parseInt(time) / 1000000) : def;
    } catch (e) {
        return def;
    }
}

module.exports.DATABASE_NAME = () => process.env.CLICKHOUSE_DB || 'cloki';
module.exports.fingerPrint = fingerPrint;
module.exports.labelParser = labelParser;
module.exports.clickParser = clickParser;
module.exports.toJSON = toJSON;
module.exports.parseMs = parseMs;
module.exports.parseOrDefault = parseOrDefault;
