/* Prometheus Remote Write Handler for cLoki */
/*

   Accepts Prometheus WriteRequest Protobuf events

   { "timeseries":[
      {
        "labels":[{"name":"test","response_code":"200"}],
        "samples":[{"value":7.1,"timestamp":"1641758471000"}]
     }]
   }

*/

function handler (req, res) {
  const self = this
  if (this.debug) console.log('POST /api/v1/prom/remote/write')
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
  if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/x-protobuf') > -1) {
    streams = req.body.timeseries
  }
  if (streams) {
    streams.forEach(function (stream) {
      let JSONLabels
      let finger = null
      try {
        // let JSONLabels
        try {
          JSONLabels = stream.labels[0]
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

      if (stream.samples) {
        stream.samples.forEach(function (entry) {
          if (self.debug) console.log('BULK ROW', entry, finger)
          if (
            !entry &&
            !entry.timestamp &&
            !entry.value
          ) {
            console.error('no bulkable data', entry)
            return
          }
          const values = [
            finger,
            entry.timestamp,
            entry.value,
            JSONLabels.name || ''
          ]
          self.bulk.add(finger, values)
        })
      }
    })
  }
  res.code(204).send()
}

module.exports = handler
