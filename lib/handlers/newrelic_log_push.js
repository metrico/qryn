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

const { QrynBadRequest } = require('./errors')
const stringify = require('../utils').stringify

async function handler (req, res) {
  const self = this
  req.log.debug('NewRelic Log Index Request')
  if (!req.body) {
    req.log.error('No Request Body')
    throw new QrynBadRequest('No request body')
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    throw new QrynBadRequest('Read only mode')
  }
  let streams
  if (Array.isArray(req.body)) {
    // Bulk Logs
    streams = req.body
  } else {
    // Single Log
    const tags = req.body
    const { timestamp, message } = tags
    if (!timestamp) {
      throw new QrynBadRequest('Log timestamp is undefined')
    }
    if (!message) {
      throw new QrynBadRequest('Log message is undefined')
    }
    delete tags.message
    delete tags.timestamp
    streams = [{
      common: { attributes: tags },
      logs: [{ timestamp, message }]
    }]
  }
  req.log.info({ streams }, 'streams')
  const promises = []
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
        promises.push(self.bulk_labels.add([[
          new Date().toISOString().split('T')[0],
          finger,
          strJson,
          JSONLabels.target || ''
        ]]))
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
            BigInt(`${log.timestamp}0000000000000000000`.substring(0, 19)),
            null,
            log.message
          ]
          promises.push(self.bulk.add([values]))
        })
      }
    })
  }
  await Promise.all(promises)
  res.code(200).send('OK')
}

module.exports = handler
