/* Telegraf Handler */
/*

[[outputs.http]]
  url = "http://cloki:3100/telegraf"
  data_format = "json"
  method = "POST"

*/

function handler(req, res){
	if (this.debug) console.log("POST /telegraf");
	if (this.debug) console.log("QUERY: ", req.query);
	if (this.debug) console.log("BODY: ", req.body);
	if (!req.body && !req.body.metrics) {
		console.error("No Request Body!", req);
		return;
	}
	if (readonly) {
		console.error("Readonly! No push support.");
		res.send(500);
		return;
	}
	var streams;
	streams = req.body.metrics;
	if (!Array.isArray(streams)) streams = [streams];
	if (streams) {
		if (this.debug) console.log("influx", streams);
		streams.forEach(function (stream) {
			try {
				var JSON_labels = stream.tags;
				JSON_labels.metric = stream.name;
				// Calculate Fingerprint
				var finger = this.fingerPrint(JSON.stringify(JSON_labels));
				if (this.debug)
					console.log("LABELS FINGERPRINT", JSON_labels, finger);
				this.labels.add(finger, stream.labels);
				// Store Fingerprint
				this.bulk_labels.add(finger, [
					new Date().toISOString().split("T")[0],
					finger,
					JSON.stringify(JSON_labels),
					stream.name || "",
				]);
				for (var key in JSON_labels) {
					//if (this.debug) console.log('Storing label',key, JSON_labels[key]);
					this.labels.add("_LABELS_", key);
					this.labels.add(key, JSON_labels[key]);
				}
			} catch (e) {
				console.log(e);
			}

			if (stream.fields) {
				Object.keys(stream.fields).forEach(function (entry) {
					// if (this.debug) console.log('BULK ROW',entry,finger);
					if (
						!entry &&
						!stream.timestamp &&
						(!entry.value || !entry.line)
					) {
						console.error("no bulkable data", entry);
						return;
					}
					var values = [
						finger,
						stream.timestamp * 1000,
						stream.fields[entry] || 0,
						stream.fields[entry].toString() || "",
					];
					this.bulk.add(finger, values);
				});
			}
		});
	}
	res.send(200);
};

module.exports = handler
