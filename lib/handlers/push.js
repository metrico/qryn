/* Push Handler */
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

function handler(req, res){
        if (this.debug) console.log("POST /loki/api/v1/push");
        if (this.debug) console.log("QUERY: ", req.query);
        if (this.debug) console.log("BODY: ", req.body);
        if (!req.body) {
                console.error("No Request Body!", req);
                res.send(500);
                return;
        }
        if (this.readonly) {
                console.error("Readonly! No push support.");
                res.send(500);
                return;
        }
        var streams;
        if (
                req.headers["content-type"] &&
                req.headers["content-type"].indexOf("application/json") > -1
        ) {
                streams = req.body.streams;
        } else if (
                req.headers["content-type"] &&
                req.headers["content-type"].indexOf("application/x-protobuf") > -1
        ) {
                // streams = messages.PushRequest.decode(req.body)
                streams = req.body;
                if (this.debug) console.log("GOT protoBuf", streams);
        }
        if (streams) {
                streams.forEach(function (stream) {
                        try {
                                try {
                                        var JSON_labels;
                                        if (stream.stream) {
                                                JSON_labels = stream.stream;
                                        } else {
                                                JSON_labels = this.toJSON(
                                                        stream.labels.replace(/\!?="/g, ':"')
                                                );
                                        }
                                } catch (e) {
                                        console.error(e);
                                        return;
                                }
                                // Calculate Fingerprint
                                var finger = this.fingerPrint(JSON.stringify(JSON_labels));
                                if (this.debug)
                                        console.log("LABELS FINGERPRINT", stream.labels, finger);
                                this.labels.add(finger, stream.labels);
                                // Store Fingerprint
                                this.bulk_labels.add(finger, [
                                        new Date().toISOString().split("T")[0],
                                        finger,
                                        JSON.stringify(JSON_labels),
                                        JSON_labels["name"] || "",
                                ]);
                                for (var key in JSON_labels) {
                                        if (this.debug)
                                                console.log("Storing label", key, JSON_labels[key]);
                                        this.labels.add("_LABELS_", key);
                                        this.labels.add(key, JSON_labels[key]);
                                }
                        } catch (e) {
                                console.log(e);
                        }

                        if (stream.entries) {
                                stream.entries.forEach(function (entry) {
                                        if (this.debug) console.log("BULK ROW", entry, finger);
                                        if (
                                                !entry &&
                                                (!entry.timestamp || !entry.ts) &&
                                                (!entry.value || !entry.line)
                                        ) {
                                                console.error("no bulkable data", entry);
                                                return;
                                        }
                                        var values = [
                                                finger,
                                                new Date(entry.timestamp || entry.ts).getTime(),
                                                entry.value || 0,
                                                entry.line || "",
                                        ];
                                        console.log(values);
                                        this.bulk.add(finger, values);
                                });
                        }

                        if (stream.values) {
                                stream.values.forEach(function (value) {
                                        if (this.debug) console.log("BULK ROW", value, finger);
                                        if (!value && !value[0] && !value[1]) {
                                                console.error("no bulkable data", value);
                                                return;
                                        }
                                        var values = [
                                                finger,
                                                Math.round(value[0] / 1000000), // convert to millieseconds
                                                0,
                                                value[1],
                                        ];
                                        this.bulk.add(finger, values);
                                });
                        }
                });
        }
        res.send(200);
};

module.exports = handler
