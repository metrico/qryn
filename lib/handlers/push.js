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

function handler (req, res) {
  const self = this
  if (this.debug) console.log('POST /loki/api/v1/push')
  if (this.debug) console.log('QUERY: ', req.query)
  if (this.debug) console.log('BODY: ', req.body)
  if (!req.body) {
    console.error('No Request Body!', req)
    res.send(500)
    return
  }
  if (this.readonly) {
    console.error('Readonly! No push support.')
    res.send(500)
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
    // streams = messages.PushRequest.decode(req.body)
    streams = req.body
    if (this.debug) console.log('GOT protoBuf', streams)
  }
  if (streams) {
    streams.forEach(function (stream) {
      let finger = null
      try {
        let JSONLabels
        try {
          if (stream.stream) {
            JSONLabels = stream.stream
          } else {
            JSONLabels = self.toJSON(
              stream.labels.replace(/\!?="/g, ':"')
            )
          }
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
          JSONLabels.name || ''
        ])
        for (const key in JSONLabels) {
          if (self.debug) { console.log('Storing label', key, JSONLabels[key]) }
          self.labels.add('_LABELS_', key)
          self.labels.add(key, JSONLabels[key])
        }
      } catch (e) {
        console.log(e)
      }

      if (stream.entries) {
        stream.entries.forEach(function (entry) {
          if (self.debug) console.log('BULK ROW', entry, finger)
          if (
            !entry &&
                                                (!entry.timestamp || !entry.ts) &&
                                                (!entry.value || !entry.line)
          ) {
            console.error('no bulkable data', entry)
            return
          }
          const values = [
            finger,
            new Date(entry.timestamp || entry.ts).getTime(),
            entry.value || 0,
            entry.line || ''
          ]
          self.bulk.add(finger, values)
        })
      }

      if (stream.values) {
        stream.values.forEach(function (value) {
          if (self.debug) console.log('BULK ROW', value, finger)
          if (!value && !value[0] && !value[1]) {
            console.error('no bulkable data', value)
            return
          }
          const values = [
            finger,
            Math.round(value[0] / 1000000), // convert to millieseconds
            0,
            value[1]
          ]
          self.bulk.add(finger, values)
        })
      }
    })
  }
  res.send(204)
}

module.exports = handler
