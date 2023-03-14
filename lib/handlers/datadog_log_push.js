
/* Datadig Log Indexing Handler
   Accepts JSON formatted requests when the header Content-Type: application/json is sent.

	POST /api/v2/logs
  
   Accepts Datadog Log JSON Body objects:
   
   [{
      ddsource: "nginx",
      ddtags: "env:staging,version:5.1",
      hostname: "i-012345678",
      message: "2019-11-19T14:37:58,995 INFO [process.name][20081] Hello World",
      service: "payment",
    }]
   

*/

const stringify = require('../utils').stringify
const tagsToObject = (data, delimiter = ',') =>
  Object.fromEntries(data.split(',').map(v => {
    const fields = v.split(':')
    return [fields[0], fields[1]]
  }))

async function handler (req, res) {
  const self = this
  req.log.debug('Datadog Log Index Request')
  if (!req.body) {
    req.log.error('No Request Body or Target!')
    return res.code(400).send('{"status":400, "error": { "reason": "No Request Body" } }')
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    return res.code(400).send('{"status":400, "error": { "reason": "Read Only Mode" } }')
  }

  let streams
  if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/json') > -1
  ) {
    // json body, handle as array
    streams = req.body
  } else {
    // raw body, handle as ndjson
    streams = req.body.split(/\n/)
  }
  req.log.info({ streams }, 'streams')
  const promises = []
  if (streams) {
    streams.forEach(function (stream) {
      req.log.debug({ stream }, 'ingesting datadog log')
      let finger = null
      let JSONLabels = stream.ddtags ? tagsToObject(stream.ddtags) : {}
      try {
        try {
          JSONLabels.type = 'datadog'
          if (stream.ddsource || req.query.ddsource) JSONLabels.ddsource = stream.ddsource || req.query.ddsource
          if (stream.source) JSONLabels.source = stream.source
          if (stream.hostname) JSONLabels.hostname = stream.hostname
          if (stream.source) JSONLabels.source = stream.source
          // sort labels
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

      // Store Datadog Log
      const values = [
        finger,
        BigInt((new Date().getTime() * 1000) + '000'),
        null,
        stream.message
      ]
      req.log.debug({ finger, values }, 'store')
      promises.push(self.bulk.add([values]))
    })
  }
  await Promise.all(promises)
  // always 202 empty JSON
  return res.code(202).send('{}')
}

module.exports = handler
