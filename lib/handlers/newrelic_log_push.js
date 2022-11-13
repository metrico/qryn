/*  New Relic Log Ingestor (https://docs.newrelic.com/docs/logs/log-api/introduction-log-api/)

    Accepts JSON formatted requests when the header Content-Type: application/json is sent.
    Example of the JSON format:

    POST /log/v1 HTTP/1.1
    Host: log-api.newrelic.com
    Content-Type: application/json
    Api-Key: <YOUR_LICENSE_KEY>
    Content-Length: 319
    [{
       "common": {
         "attributes": {
           "logtype": "accesslogs",
           "service": "login-service",
           "hostname": "login.example.com"
         }
       },
       "logs": [{
           "timestamp": <TIMESTAMP_IN_UNIX_EPOCH><,
           "message": "User 'xyz' logged in"
         },{
           "timestamp": <TIMESTAMP_IN_UNIX_EPOCH,
           "message": "User 'xyz' logged out",
           "attributes": {
             "auditId": 123
           }
         }]
    }]
*/

const stringify = require('../utils').stringify

function handler (req, res) {
  const self = this
  req.log.debug('NewRelic Log Index Request')
  if (!req.body || !req.params.target) {
    req.log.error('No Request Body or Target!')
    res.code(400).send('{"status":400, "error": { "reason": "No Request Body" } }')
    return
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    res.code(400).send('{"status":400, "error": { "reason": "Read Only Mode" } }')
    return
  }
  
  let streams;
  if (Array.isArray(req.body)){
     // Bulk Logs
     streams = req.body 
  } else {
     // Single Log
     var {timestamp, ...tags } = req.body;
     var {message, ...tags} = tags;
     streams = [{ 
        attributes: tags, 
        logs: [{ timestamp, message }]
     }]
  }
  req.log.info({ streams }, 'streams')
  if (streams) {
    streams.forEach(function (stream) {
      req.log.debug({ stream }, 'ingesting newrelic log')
      let finger = null
      let JSONLabels = stream?.common?.attributes || stream?.attributes || {}
      try {
        try {
          JSONLabels.type = 'newrelic'
          JSONLabels = Object.fromEntries(Object.entries(JSONLabels).sort())
        } catch (err) {
          req.log.error({ err })
          return
        }
        // Calculate Fingerprint
        const strJson = stringify(JSONLabels)
        finger = self.fingerPrint(strJson)
        // Store Fingerprint
        self.bulk_labels.add([[
          new Date().toISOString().split('T')[0],
          finger,
          strJson,
          JSONLabels.target || ''
        ]])
        for (const key in JSONLabels) {
          req.log.debug({ key, data: JSONLabels[key] }, 'Storing label')
          self.labels.add('_LABELS_', key)
          self.labels.add(key, JSONLabels[key])
        }
      } catch (err) {
        req.log.error({ err }, 'failed ingesting datadog log')
      }
      // Queue Array logs
      if (stream.logs) {
        stream.logs.forEach(function (log) {
            // Store NewRelic Log
            // TODO: handle additional attributes!
            const values = [
              finger,
              BigInt((new Date(log.timestamp).getTime() * 1000) + '000'),
              null,
              log.message
            ]
            req.log.debug({ finger, values }, 'store')
            self.bulk.add([values])
        })
      }
    })
  }
  res.code(200).send()
}

module.exports = handler
