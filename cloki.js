#!/usr/bin/env node

/*
 * Loki API to Clickhouse Gateway
 * (C) 2018-2021 QXIP BV
 */

this.debug = process.env.DEBUG || false;
var debug = this.debug;

this.readonly = process.env.READONLY || false;
this.http_user = process.env.CLOKI_LOGIN || false;
this.http_password = process.env.CLOKI_PASSWORD || false;

require("./plugins/engine");

var DATABASE = require("./lib/db/clickhouse");
var UTILS = require("./lib/utils");
const snappy = require("snappy");

/* ProtoBuf Helper */
var fs = require("fs");
var protoBuff = require("protocol-buffers");
var messages = protoBuff(fs.readFileSync("lib/loki.proto"));

/* Fingerprinting */
this.fingerPrint = UTILS.fingerPrint;
this.toJSON = UTILS.toJSON;

/* Database this.bulk Helpers */
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

if (!this.readonly) init(process.env.CLICKHOUSE_DB || "cloki");

/* Fastify Helper */
const fastify = require("fastify")({
    logger: false,
});

fastify.register(require("fastify-url-data"));
fastify.register(require('fastify-websocket'));

fastify.after((err) => {
    if (err) throw err;
});

/* Enable Simple Authentication */
if (this.http_user && this.http_password) {
    function checkAuth(username, password, req, reply, done) {
      if (username === this.http_user && password === this.http_password) {
        done();
      } else {
        done(new Error("Unauthorized!: Wrong username/password."));
      }
    }
    const validate = checkAuth.bind(this);

    fastify.register(require("fastify-basic-auth"), {
        validate
    });
    fastify.after(() => {
        fastify.addHook("preHandler", fastify.basicAuth);
    });
}

fastify.addContentTypeParser("text/plain", {
    parseAs: "string"
}, function(req, body, done) {
    try {
        var json = JSON.parse(body);
        done(null, json);
    } catch (err) {
        err.statusCode = 400;
        done(err, undefined);
    }
});

/* Protobuf Handler */
fastify.addContentTypeParser("application/x-protobuf", {parseAs: 'buffer'},
    async function (req, body, done) {
    let _data = await snappy.uncompress(body);
    _data = messages.PushRequest.decode(_data);
    _data.streams = _data.streams.map(s => ({
        ...s,
        entries: s.entries.map(e => {
            let millis = Math.floor(e.timestamp.nanos / 1000000);
            return {
                ...e,
                timestamp: e.timestamp.seconds * 1000 + millis,
            };
            })
    }));
    return _data.streams;
});

/* 404 Handler */
const handler_404 = require('./lib/handlers/404.js').bind(this);
fastify.setNotFoundHandler(handler_404);

/* Hello cloki test API */
const handler_hello = require('./lib/handlers/hello.js').bind(this);
fastify.get("/hello", handler_hello);

/* Write Handler */
const handler_push = require('./lib/handlers/push.js').bind(this);
fastify.post("/loki/api/v1/push", handler_push);

/* Telegraf HTTP Bulk handler */
const handler_telegraf = require('./lib/handlers/telegraf.js').bind(this);
fastify.post("/telegraf", handler_telegraf);

/* Query Handler */
const handler_query_range = require('./lib/handlers/query_range.js').bind(this);
fastify.get("/loki/api/v1/query_range", handler_query_range);

/* Label Handlers */
/* Label Value Handler via query (test) */
const handler_query = require('./lib/handlers/query.js').bind(this);
fastify.get("/loki/api/v1/query", handler_query);

/* Label Handlers */
const handler_label = require('./lib/handlers/label.js').bind(this);
fastify.get("/loki/api/v1/label", handler_label);
fastify.get("/loki/api/v1/labels", handler_label);

/* Label Value Handler */
const handler_label_values = require('./lib/handlers/label_values.js').bind(this);
fastify.get("/loki/api/v1/label/:name/values", handler_label_values);

/* Series Placeholder - we do not track this as of yet */
const handler_series = require('./lib/handlers/series.js').bind(this);
fastify.get("/loki/api/v1/series", handler_series);

fastify.get('/loki/api/v1/tail', {websocket:true}, require('./lib/handlers/tail').bind(this));

// Run API Service
fastify.listen(
    process.env.PORT || 3100,
    process.env.HOST || "0.0.0.0",
    (err, address) => {
        if (err) throw err;
        console.log("cLoki API up");
        fastify.log.info(`cloki API listening on ${address}`);
    }
);

module.exports.stop = () => {
    fastify.close();
    DATABASE.stop();
};
