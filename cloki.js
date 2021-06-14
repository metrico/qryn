/*
 * Loki API to Clickhouse Gateway
 * (C) 2018-2021 QXIP BV
 */

/* TODO: split into modules and prioritize performance! contributors help yourselves :) */

this.debug = process.env.DEBUG || false;
var debug = this.debug;

this.readonly = process.env.READONLY || false;
this.http_user = process.env.CLOKI_LOGIN || false;
this.http_pass = process.env.CLOKI_PASSWORD || false;

var DATABASE = require("./lib/db/clickhouse");
var UTILS = require("./lib/utils");

/* ProtoBuf Helper */
var fs = require("fs");
var protoBuff = require("protocol-buffers");
var messages = protoBuff(fs.readFileSync("lib/loki.proto"));

/* Fingerprinting */
var fingerPrint = UTILS.fingerPrint;
this.toJSON = UTILS.toJSON;

// Database this.bulk Helpers */
this.bulk = DATABASE.cache.bulk; // samples
this.bulk_labels = DATABASE.cache.bulk_labels; // labels
this.labels = DATABASE.cache.labels; // in-memory labels

/* Function Helpers */
this.labelParser = UTILS.labelParser;

var init = DATABASE.init;
this.reloadFingerprints = DATABASE.reloadFingerprints;
this.scanFingerprints = DATABASE.scanFingerprints;
this.scanMetricFingerprints = DATABASE.scanMetricFingerprints;
this.scanClickhouse = DATABASE.scanClickhouse;

if (!this.readonly) init(process.env.CLICKHOUSE_TSDB || "loki");

/* Fastify Helper */
const fastify = require("fastify")({
	logger: false,
});

const path = require("path");

fastify.register(require("fastify-url-data"), (err) => {
	if (err) throw err;
});

const handler_404 = require('./lib/handlers/404.js').bind(this);
fastify.setNotFoundHandler(handler_404);

/* Enable Simple Authentication */
if (this.http_ && this.http_password) {
	fastify.register(require("fastify-basic-auth"), { validate });
	fastify.after(() => {
		fastify.addHook("preHandler", fastify.basicAuth);
	});
}

function validate(username, password, req, reply, done) {
	if (username === this.http_user && password === this.http_password) {
		done();
	} else {
		done(new Error("Unauthorized!: Wrong username/password."));
	}
}

fastify.addContentTypeParser("text/plain", { parseAs: "string" }, function (
	req,
	body,
	done
) {
	try {
		var json = JSON.parse(body);
		done(null, json);
	} catch (err) {
		err.statusCode = 400;
		done(err, undefined);
	}
});

fastify.addContentTypeParser("application/x-protobuf", function (req, done) {
	var data = "";
	req.on("data", (chunk) => {
		data += chunk;
	});
	req.on("error", (error) => {
		console.log(error);
	});
	req.on("end", () => {
		done(messages.PushRequest.decode(data));
	});
});

const handler_hello = require('./lib/handlers/hello.js').bind(this);
fastify.get("/hello", handler_hello);

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
const handler_push = require('./lib/handlers/push.js').bind(this);
fastify.post("/loki/api/v1/push", handler_push);

/* Telegraf HTTP Bulk handler */
const handler_telegraf = require('./lib/handlers/telegraf.js').bind(this);
fastify.post("/telegraf", handler_telegraf);

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

const handler_query_range = require('./lib/handlers/query_range.js').bind(this);
fastify.get("/loki/api/v1/query_range", handler_query_range);

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
var handler_query = require('./lib/handlers/query.js').bind(this);
fastify.get("/loki/api/v1/query", handler_query);

var handler_label = require('./lib/handlers/label.js').bind(this);
fastify.get("/loki/api/v1/label", handler_label);
fastify.get("/loki/api/v1/labels", handler_label);

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

var handler_label_values = require('./lib/handlers/label_values.js').bind(this);
fastify.get("/loki/api/v1/label/:name/values", handler_label_values);

/* Series Placeholder - we do not track this as of yet */
var handler_series = require('./lib/handlers/series.js').bind(this);
fastify.get("/loki/api/v1/series", handler_series);

// Run API Service
fastify.listen(
	process.env.PORT || 3100,
	process.env.HOST || "0.0.0.0",
	(err, address) => {
		if (err) throw err;
		console.log("cLoki API up");
		fastify.log.info(`server listening on ${address}`);
	}
);
