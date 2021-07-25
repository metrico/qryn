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

async function handler(req, res){
        if (this.debug) console.log("GET /loki/api/v1/query_range");
        if (this.debug) console.log("QUERY: ", req.query);
        // console.log( req.urlData().query.replace('query=',' ') );
        var params = req.query;
        var resp = { streams: [] };
        if (!req.query.query) {
                res.send(resp);
                return;
        }
        /* remove newlines */
        req.query.query = req.query.query.replace(/\n/g, " ");
        /* query templates */
        var RATEQUERY = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*)/;
        var RATEQUERYWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*) where (.*)/;
        var RATEQUERYNOWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.([\S]+)\s?(.*)/;
        var RATEQUERYMETRICS = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\)/;

        if (!req.query.query) {
                res.code(400).send("invalid query");
        } else if (RATEQUERYNOWHERE.test(req.query.query)) {
                var s = RATEQUERYNOWHERE.exec(req.query.query);
                console.log("tags", s);
                var JSON_labels = {
                        db: s[5],
                        table: s[6],
                        interval: s[4] || 60,
                        tag: s[2],
                        metric: s[1] + "(" + s[3] + ")",
                        where: s[3] + " " + s[7],
                };
                this.scanClickhouse(JSON_labels, res, params);
        } else if (RATEQUERYWHERE.test(req.query.query)) {
                var s = RATEQUERYWHERE.exec(req.query.query);
                console.log("tags", s);
                var JSON_labels = {
                        db: s[5],
                        table: s[6],
                        interval: s[4] || 60,
                        tag: s[2],
                        metric: s[1] + "(" + s[3] + ")",
                        where: s[7],
                };
                this.scanClickhouse(JSON_labels, res, params);
        } else if (RATEQUERY.test(req.query.query)) {
                var s = RATEQUERY.exec(req.query.query);
                console.log("tags", s);
                var JSON_labels = {
                        db: s[5],
                        table: s[6],
                        interval: s[4] || 60,
                        tag: s[2],
                        metric: s[1] + "(" + s[3] + ")",
                };
                this.scanClickhouse(JSON_labels, res, params);
        } else if (RATEQUERYMETRICS.test(req.query.query)) {
                var s = RATEQUERYMETRICS.exec(req.query.query);
                console.log("metrics tags", s);
                var JSON_labels = {
                        db: "loki",
                        table: "samples",
                        interval: s[4] || 60,
                        tag: s[2],
                        metric: s[1] + "(" + s[3] + ")",
                };
                this.scanMetricFingerprints(JSON_labels, res, params);
        } else if (req.query.query.startsWith("clickhouse(")) {
                try {
                        var query =
                                /\{(.*?)\}/g.exec(req.query.query)[1] || req.query.query;
                        var queries = query.replace(/\!?="/g, ':"');
                        var JSON_labels = this.toJSON(queries);
                } catch (e) {
                        console.error(e, queries);
                        res.send(resp);
                }
                if (this.debug) console.log("SCAN CLICKHOUSE", JSON_labels, params);
                this.scanClickhouse(JSON_labels, res, params);
        } else {
                try {
                    await this.scanFingerprints(
                        req.query,
                        res,
                    );
                } catch (e) {
                        res.send(resp);
                }
                if (this.debug) console.log("SCAN LABELS", JSON_labels, label_rules, params);

        }
};

module.exports = handler
