/* Prometheus Remote Write Handler for Qryn */
/*

   Accepts Prometheus WriteRequest Protobuf events

   { "timeseries":[
      {
        "labels":[{"name":"test","response_code":"200"}],
        "samples":[{"value":7.1,"timestamp":"1641758471000"}]
     }]
   }

*/
const { asyncLogError } = require('../../common')
const stringify = require('../utils').stringify
const DATABASE = require('../db/clickhouse')
const { bulk_labels, bulk, labels } = DATABASE.cache
const { fingerPrint } = require('../utils')
const { readonly } = require('../../common')

async function handler (req, res) {
  const self = this
  req.log.debug('POST /api/v1/prom/remote/write')
  if (!req.body) {
    asyncLogError('No Request Body!', req.log)
    return res.code(500).send()
  }
  if (readonly) {
    asyncLogError('Readonly! No push support.', req.log)
    return res.code(500).send()
  }
  let streams
  if (req.headers['content-type'] && req.headers['content-type'].indexOf('application/x-protobuf') > -1) {
    streams = req.body.timeseries
  }
  const promises = []
  if (streams) {
    streams.forEach(function (stream) {
      let JSONLabels
      let finger = null
      try {
        JSONLabels = stream.labels.reduce((sum, l) => {
          sum[l.name] = l.value
          return sum
        }, {})
        // Calculate Fingerprint
        const strJson = stringify(JSONLabels)
        finger = fingerPrint(strJson)
        req.log.debug({ labels: stream.labels, finger }, 'LABELS FINGERPRINT')
        labels.add(finger.toString(), stream.labels)

        const dates = {}
        if (stream.samples) {
          stream.samples.forEach(function (entry) {
            req.log.debug({ entry, finger }, 'BULK ROW')
            if (
              !entry &&
              !entry.timestamp &&
              !entry.value
            ) {
              asyncLogError({ entry }, req.log)
              return
            }
            const ts = BigInt(entry.timestamp)
            const values = [
              finger,
              ts,
              entry.value,
              JSONLabels.__name__ || 'undefined'
            ]
            dates[
              new Date(parseInt((ts / BigInt('1000000')).toString())).toISOString().split('T')[0]
            ] = 1
            promises.push(bulk.add([values]))
          })
        }
        for (const d of Object.keys(dates)) {
          // Store Fingerprint
          promises.push(bulk_labels.add([[
            d,
            finger,
            strJson,
            JSONLabels.__name__ || 'undefined'
          ]]))
          for (const key in JSONLabels) {
            labels.add('_LABELS_', key)
            labels.add(key, JSONLabels[key])
          }
        }
      } catch (err) {
        asyncLogError(err, req.log)
        return res.code(500).send()
      }
    })
  }
  await Promise.all(promises)
  return res.code(204).send()
}

module.exports = handler
