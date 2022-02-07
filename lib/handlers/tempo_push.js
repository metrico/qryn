/* Zipkin Push Handler */
/*
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.
    Example of the Zipkin span JSON format:
    [{
	 "id": "1234",
	 "traceId": "0123456789abcdef",
	 "timestamp": 1608239395286533,
	 "duration": 100000,
	 "name": "span from bash!",
	 "tags": {
		"http.method": "GET",
		"http.path": "/api"
	  },
	  "localEndpoint": {
		"serviceName": "shell script"
	  }
	}]
*/

function handler (req, res) {
  const self = this
  req.log.debug('POST /tempo/api/push')
  if (!req.body) {
    req.log.error('No Request Body!')
    res.code(500).send()
    return
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    res.code(500).send()
    return
  }
  let streams
  if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/json') > -1
  ) {
    streams = req.body
  } else if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/x-protobuf') > -1
  ) {
    streams = req.body
    req.log.debug({ streams }, 'GOT protoBuf')
  } else {
    streams = req.body
  }
  req.log.info({ streams }, 'streams');
  if (streams) {
    streams.forEach(function (stream) {
      req.log.debug({ stream }, 'ingesting tempo stream');
      let finger = null
      try {
        let JSONLabels = {}
        try {
	  JSONLabels.type = "tempo";
	  if (this.tempo_tagtrace) JSONLabels.traceId = stream.traceId;
	  if (stream.parentId) JSONLabels.parentId = stream.parentId;
	  if (stream.localEndpoint){
	    for (const key in stream.localEndpoint) {
	      JSONLabels[key] = stream.localEndpoint[key];
	    }
	  }
	  JSONLabels = Object.fromEntries(Object.entries(JSONLabels).sort())
        } catch (err) {
          req.log.error({ err })
          return
        }
        // Calculate Fingerprint
        finger = self.fingerPrint(JSON.stringify(JSONLabels))
        req.log.debug({ JSONLabels, finger }, 'LABELS FINGERPRINT')
        self.labels.add(finger, stream.labels)
        // Store Fingerprint
        self.bulk_labels.add([[
          new Date().toISOString().split('T')[0],
          finger,
          JSON.stringify(JSONLabels),
          JSONLabels.traceId || ''
        ]])
        for (const key in JSONLabels) {
          req.log.debug({ key, data: JSONLabels[key] }, 'Storing label')
          self.labels.add('_LABELS_', key)
          self.labels.add(key, JSONLabels[key])
        }
      } catch (err) {
        req.log.error({ err }, 'failed ingesting tempo stream')
      }

      // Form Tempo Object - or shall we materialize this from CH?
      try {
        var tags = Object.keys(stream.tags || {}).map(function(key){ return {key: key, value: typeof stream.tags[key] === 'string' ? stream.tags[key] : parseInt(stream.tags[key]), type: typeof stream.tags[key] == "string" ? "string" : "int64"}; });
        var tempo = {
          "traceID": stream.traceId,
          "spanID": stream.id,
          "name": stream.name,
          "references": stream.references || [],
          "startTime": stream.timestamp,
	  "startTimeUnixNano": parseInt(stream.timestamp * 1000),
	  "endTimeUnixNano": parseInt(stream.timestamp * 1000) + parseInt(stream.duration * 1000 || 1000),
          "duration": parseInt(stream.duration) || 1000,
          "tags": tags || [],
          "logs": [],
          "processID": stream.processID || "p1",
          "warnings": null
        }
	if(stream.localEndpoint) tempo.localEndpoint = stream.localEndpoint; // already in tags, deduplicate?
	if(stream.parentId) tempo.parentSpanID = stream.parentId;
	req.log.debug(tempo)
	tempo = JSON.parse(JSON.stringify(tempo));
      } catch(err) { req.log.error({ err }); return };
      // Store Tempo Object
      const values = [
            finger,
            parseInt(stream.timestamp/1000) || new Date().getTime(),
            parseInt(stream.duration) || null,
            JSON.stringify(tempo) || '' ]
      req.log.debug({ finger, values }, 'store');
      self.bulk.add([values])
    })
  }
  res.code(204).send()
}

module.exports = handler
