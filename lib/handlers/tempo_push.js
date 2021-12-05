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
  if (this.debug) console.log('POST /tempo/api/push')
  if (this.debug) console.log('QUERY: ', req.query)
  if (this.debug) console.log('BODY: ', req.body)
  if (!req.body) {
    console.error('No Request Body!', req)
    res.code(500).send()
    return
  }
  if (this.readonly) {
    console.error('Readonly! No push support.')
    res.code(500).send()
    return
  }
  let streams
  if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/json') > -1
  ) {
    streams = req.body.streams
  } else if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/x-protobuf') > -1
  ) {
    streams = req.body
    if (this.debug) console.log('GOT protoBuf', streams)
  }
  if (streams) {
    streams.forEach(function (stream) {
      let finger = null
      try {
        let JSONLabels = {}
        try {
          JSONLabels.id = stream.id;
		  JSONLabels.traceId = stream.traceId;
		  if (stream.parentId) JSONLabels.parentId = stream.parentId;
        } catch (e) {
          console.error(e)
          return
        }
        // Calculate Fingerprint
        finger = self.fingerPrint(JSON.stringify(JSONLabels))
        if (self.debug) { console.log('LABELS FINGERPRINT', stream.labels, finger) }
        self.labels.add(finger, stream.labels)
        // Store Fingerprint
        self.bulk_labels.add(finger, [
          new Date().toISOString().split('T')[0],
          finger,
          JSON.stringify(JSONLabels),
          JSONLabels.traceId || ''
        ])
        for (const key in JSONLabels) {
          if (self.debug) { console.log('Storing label', key, JSONLabels[key]) }
          self.labels.add('_LABELS_', key)
          self.labels.add(key, JSONLabels[key])
        }
      } catch (e) {
        console.log(e)
      }
	  
	  // Store Tempo Object
      const values = [
            finger,
            stream.timestamp || new Date(entry.timestamp || entry.ts).getTime(),
            (typeof entry.value === 'undefined') ? null : entry.value,
            JSON.stringify(stream) || ''
          ]
      self.bulk.add(finger, values)
    }
  }
  res.code(204).send()
}

module.exports = handler
