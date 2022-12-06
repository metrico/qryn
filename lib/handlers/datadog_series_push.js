/* Datadig Series Indexing Handler

   Accepts JSON formatted requests when the header Content-Type: application/json is sent.
   
   {
    "series": [
      {
        "metric": "system.load.1",
        "type": 0,
        "points": [
          {
            "timestamp": 1636629071,
            "value": 0.7
          }
        ],
        "resources": [
          {
            "name": "dummyhost",
            "type": "host"
          }
        ]
      }
    ]
  }
  
*/
const stringify = require('../utils').stringify

async function handler (req, res) {
  const self = this
  req.log.debug('Datadog Series Index Request')
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
  if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/json') > -1){
    streams = req.body.series
  }
  const promises = []
  if (streams) {
    streams.forEach(function (stream) {
      let JSONLabels
      let finger = null
      try {
        try {
          JSONLabels = stream.resources
          JSONLabels['__name__'] = stream.metric
          JSONLabels = Object.fromEntries(Object.entries(JSONLabels).sort())
        } catch (err) {
          req.log.error({ err })
          return
        }
        // Calculate Fingerprint
        const strJson = stringify(JSONLabels)
        finger = self.fingerPrint(strJson)
        self.labels.add(finger.toString(), stream.labels)
        // Store Fingerprint
        promises.push(self.bulk_labels.add([[
          new Date().toISOString().split('T')[0],
          finger,
          strJson,
          JSONLabels['__name__'] || 'undefined'
        ]]))
        for (const key in JSONLabels) {
          self.labels.add('_LABELS_', key)
          self.labels.add(key, JSONLabels[key])
        }
      } catch (err) {
        req.log.error({ err })
      }

      if (stream.points) {
        stream.points.forEach(function (entry) {
          req.log.debug({ entry, finger }, 'BULK ROW')
          if (
            !entry &&
            !entry.timestamp &&
            !entry.value
          ) {
            req.log.error({ entry }, 'no bulkable data')
            return
          }
          const values = [
            finger,
            BigInt(pad('0000000000000000000', entry.timestamp)),
            entry.value,
            JSONLabels['__name__'] || 'undefined'
          ]
          promises.push(self.bulk.add([values]))
        })
      }
    })
  }
  await Promise.all(promises)
  res.code(202).send({"errors": []})
}

function pad (pad, str, padLeft) {
  if (typeof str === 'undefined') {
    return pad
  }
  if (padLeft) {
    return (pad + str).slice(-pad.length)
  } else {
    return (str + pad).substring(0, pad.length)
  }
}

module.exports = handler
