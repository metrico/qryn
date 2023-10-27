/* Telegraf Handler */
/*

[[outputs.http]]
  url = "http://qryn:3100/telegraf"
  data_format = "json"
  method = "POST"

*/

// CANDIDATE TO DELETE DUE TO REPLACE WITH INFLUX_WRITE

const { asyncLogError } = require('../../common')
const stringify = require('../utils').stringify
const DATABASE = require('../db/clickhouse')
const { bulk_labels, bulk, labels } = DATABASE.cache
const { fingerPrint } = require('../utils')
const { readonly } = require('../../common')

function handler (req, res) {
  if (!req.body && !req.body.metrics) {
    asyncLogError('No Request Body!', req.log)
    return
  }
  if (readonly) {
    asyncLogError('Readonly! No push support.', req.log)
    return res.send(500)
  }
  let streams
  streams = req.body.metrics
  if (!Array.isArray(streams)) streams = [streams]
  if (streams) {
    req.log.debug({ streams }, 'influx')
    streams.forEach(function (stream) {
      let JSONLabels
      let finger = null
      try {
        JSONLabels = stream.tags
        JSONLabels.metric = stream.name
        // Calculate Fingerprint
        const strLabels = stringify(JSONLabels)
        finger = fingerPrint(strLabels)
        req.log.debug({ JSONLabels, finger }, 'LABELS FINGERPRINT')
        labels.add(finger.toString(), stream.labels)
        // Store Fingerprint
        bulk_labels.add(finger.toString(), [
          new Date().toISOString().split('T')[0],
          finger,
          strLabels,
          stream.name || ''
        ])
        for (const key in JSONLabels) {
          // req.log.debug({ key, data: JSONLabels[key] }, 'Storing label');
          labels.add('_LABELS_', key)
          labels.add(key, JSONLabels[key])
        }
      } catch (err) {
        asyncLogError(err, req.log)
      }
      if (stream.fields) {
        Object.keys(stream.fields).forEach(function (entry) {
          // req.log.debug({ entry, finger }, 'BULK ROW');
          if (
            !entry &&
            !stream.timestamp &&
            (!entry.value || !entry.line)
          ) {
            asyncLogError('no bulkable data', req.log)
            return
          }
          const values = [
            finger,
            BigInt(stream.timestamp + '000000000'),
            stream.fields[entry] || 0,
            stream.fields[entry].toString() || ''
          ]
          bulk.add(values)
        })
      }
    })
  }
  return res.send(200)
}

module.exports = handler
