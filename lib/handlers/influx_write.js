/* Influx Line protocol Write Handler for Qryn */
/*
   Accepts Line protocols parsed by @qxip/influx-line-protocol-parser
   
   /* metrics */
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
   
   /* syslog */
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
var lineToJSON = require('@qxip/influx-line-protocol-parser');

function handler (req, res) {
  req.log.error('POST INFLUX /write')
  if (!req.body && !req.body.metrics) {
    req.log.error('No Request Body!')
    return
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    res.send(500)
    return
  }
  let streams
  try {
    streams = lineToJSON(req.body);
  } catch(e) { streams = []; }
  
  if (!Array.isArray(streams)) streams = [streams]
  if (streams) {
    req.log.debug({ streams }, 'influx')
    streams.forEach(function (stream) {
      let JSONLabels = {};
      let JSONFields = {};
      let finger = null
      try {
        if (stream.tags){
          stream.tags.forEach(function (tag) {
            Object.assign(JSONLabels, tag);
          })
        }
        if (stream.fields){
          stream.fields.forEach(function (field) {
            Object.assign(JSONFields, field);
          })
        }
        JSONLabels['__name__'] = stream.measurement || 'null'
        // Calculate Fingerprint
        const strLabels = stringify(Object.fromEntries(Object.entries(JSONLabels).sort()))
        finger = this.fingerPrint(strLabels)
        req.log.debug({ JSONLabels, finger }, 'LABELS FINGERPRINT')
        this.labels.add(finger.toString(), stream.labels)
        // Store Fingerprint
        this.bulk_labels.add(finger.toString(), [
          new Date().toISOString().split('T')[0],
          finger,
          strLabels,
          stream.measurement || ''
        ])
        for (const key in JSONLabels) {
          // req.log.debug({ key, data: JSONLabels[key] }, 'Storing label');
          this.labels.add('_LABELS_', key)
          this.labels.add(key, JSONLabels[key])
        }
      } catch (err) {
        req.log.error({ err })
      }
      /* metrics */
      if (stream.fields && stream.measurement != "syslog") {
        for (const [key, value] of Object.entries(JSONFields)) {
          // req.log.debug({ key, value, finger }, 'BULK ROW');
          if (
            !key &&
            !stream.timestamp &&
            !value
          ) {
            req.log.error({ key }, 'no bulkable data')
            return
          }
          const values = [
            finger,
            BigInt(pad('0000000000000000000',stream.timestamp,true)),
            value || 0,
            key || ''
          ]
          this.bulk.add(values)
        }
      /* logs or syslog */  
      } else if (stream.measurement == "syslog" || JSONFields.message){
          // Send fields as a JSON object for qryn to parse
          const message = JSON.stringify(JSONFields);
          const values = [
            finger,
            BigInt(pad('0000000000000000000',stream.timestamp,true)),
            '',
            message
          ]
          this.bulk.add(values)
      }
    })
  }
  res.send(200)
}

function pad(pad, str, padLeft) {
  if (typeof str === 'undefined')
    return pad;
  if (padLeft) {
    return (pad + str).slice(-pad.length);
  } else {
    return (str + pad).substring(0, pad.length);
  }
}

module.exports = handler
