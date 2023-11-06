/* Influx Line protocol Write Handler for Qryn */
/*
   Accepts Line protocols parsed by @qxip/influx-line-protocol-parser
   
   {
     measurement: 'cpu_load_short',
     timestamp: 1422568543702900257,
     fields: [{
        value: 2
     }],
     tags:[
        {direction: 'in'},
        {host: 'server01'},
        {region: 'us-west'},
     ]
   }
   
   {
     measurement:"syslog",
     fields:[
        {facility_code: 14},
        {message: "warning message here"},
        {severity_code: 4},
        {procid: "12345"},
        {timestamp: 1534418426076077000},
        {version: 1}
     ],
     tags:[
        {appname: "myapp"},
        {facility: "console"},
        {host: "myhost"},
        {hostname: "myhost"},
        {severity: "warning"}
     ]
   }
   
*/

const stringify = require('../utils').stringify
const influxParser = require('../influx')
const { asyncLogError, errors } = require('../../common')
const DATABASE = require('../db/clickhouse')
const { bulk_labels, bulk, labels } = DATABASE.cache
const { fingerPrint } = require('../utils')
const { readonly } = require('../../common')

async function handler (req, res) {
  if (!req.body && !req.body.metrics) {
    asyncLogError('No Request Body!', req.log)
    return
  }
  if (readonly) {
    asyncLogError('Readonly! No push support.', req.log)
    return res.code(500).send('')
  }
  await influxParser.init()
  let streams = null
  try {
    streams = influxParser.parse(req.body)
  } catch (e) {
    throw new errors.QrynBadRequest(e.toString())
  }
  const promises = []
  if (streams) {
    streams.forEach(function (stream) {
      let JSONLabels = {}
      let JSONFields = {}
      let finger = null
      try {
        if (stream.tags) {
          JSONLabels = stream.tags
        }
        if (stream.fields) {
          JSONFields = stream.fields
        }
        if (stream.measurement && stream.measurement !== 'syslog' && !JSONFields.message) {
          JSONLabels.__name__ = stream.measurement || 'null'
        }
        // Calculate Fingerprint
        const strLabels = stringify(Object.fromEntries(Object.entries(JSONLabels).sort()))
        finger = fingerPrint(strLabels)
        labels.add(finger.toString(), stream.labels)
        // Store Fingerprint
        bulk_labels.add([[
          new Date().toISOString().split('T')[0],
          finger,
          strLabels,
          stream.measurement || ''
        ]])
        for (const key in JSONLabels) {
          // req.log.debug({ key, data: JSONLabels[key] }, 'Storing label');
          labels.add('_LABELS_', key)
          labels.add(key, JSONLabels[key])
        }
      } catch (err) {
        asyncLogError(err, req.log)
      }
      const timestamp = stream.timestamp || JSONFields.timestamp
      /* metrics */
      if (stream.fields && stream.measurement !== 'syslog' && !JSONFields.message) {
        for (const [key, value] of Object.entries(JSONFields)) {
          // req.log.debug({ key, value, finger }, 'BULK ROW');
          if (
            !key &&
            !timestamp &&
            !value
          ) {
            asyncLogError('no bulkable data', req.log)
            return res.code(204).send('')
          }
          const values = [
            finger,
            BigInt(pad('0000000000000000000', timestamp, true)),
            value || 0,
            key || ''
          ]
          bulk.add([values])
        }
      /* logs or syslog */
      } else if (stream.measurement === 'syslog' || JSONFields.message) {
        // Send fields as a JSON object for qryn to parse
        // const message = JSON.stringify(JSONFields)
        const values = [
          finger,
          BigInt(pad('0000000000000000000', timestamp)),
          null,
          JSONFields.message
        ]
        bulk.add([values])
      }
    })
  }
  await Promise.all(promises)
  return res.code(204).send('')
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
