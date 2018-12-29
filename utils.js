/* Function Helpers */

/* Label Parser */
var labelParser = function(labels){
	// Label Parser
	var rx = /\"?\b(\w+)\"?(!?=~?)("[^"\n]*?")/g;
	var matches, output = [];
	while (matches = rx.exec(labels)) {
	    if(matches.length >3) output.push([matches[1],matches[2],matches[3].replace(/['"]+/g, '')]);
	}
	return output;
}
/* Fingerprinting */
var shortHash = require("short-hash")
var fingerPrint = function(text,hex){
        if (hex) return shortHash(text);
        else return parseInt(shortHash(text), 16);
}

const toJSON = require('jsonic');

module.exports.fingerPrint = fingerPrint;
module.exports.labelParser = labelParser;
module.exports.toJSON = toJSON;
