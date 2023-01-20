/* Elastic Indexing Handler
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.

	POST /_bulk
	POST /<target>/_bulk

*/

const { asyncLogError } = require('../../common')
const stringify = require('../utils').stringify

function handler (req, res) {
  const self = this
  req.log.debug('ELASTIC Bulk Request')
  if (!req.body) {
    asyncLogError('No Request Body or Target!' + req.body, req.log)
    return res.code(400).send('{"status":400, "error": { "reason": "No Request Body" } }')
    return
  }
  if (this.readonly) {
    asyncLogError('Readonly! No push support.', req.log)
    return res.code(400).send('{"status":400, "error": { "reason": "Read Only Mode" } }')
  }

  const doc_target = req.params.target || false;

  let streams
  if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/x-ndjson') > -1
  ) {
    // ndjson body
    streams = req.body.split(/\n/)
  } else {
    // assume ndjson raw body
    streams = req.body.split(/\n/)
  }
  req.log.debug({ streams}, streams.lenght + ' bulk streams')
  var last_tags = false;
  if (streams) {
    streams.forEach(function (stream) {
      try {
        stream = JSON.parse(stream)
      } catch (err) { asyncLogError(err, req.log); return };

      // Allow Index, Create. Discard Delete, Update.
      if (stream.delete||stream.update) { last_tags = false; return; }
      var command = stream.index || stream.create || false;
      if (command && !last_tags) {
	      last_tags=stream.index;
	      return;
      }

      // Data Rows
      let finger = null
      let JSONLabels = last_tags;
      try {
        try {
          JSONLabels.type = 'elastic'
          if (doc_target) JSONLabels._index = doc_target
          JSONLabels = Object.fromEntries(Object.entries(JSONLabels).sort())
        } catch (err) {
          asyncLogError(err, req.log)
          return
        }
        // Calculate Fingerprint
        const strJson = stringify(JSONLabels)
        finger = self.fingerPrint(strJson)
        req.log.debug({ JSONLabels, finger }, 'LABELS FINGERPRINT')
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
        asyncLogError(err, req.log)
      }

      // Store Elastic Doc Object
      const values = [
        finger,
        BigInt((new Date().getTime() * 1000) + '000'),
        null,
        JSON.stringify(stream) || stream
      ]
      req.log.debug({ finger, values }, 'store')
      self.bulk.add([values])

      // Reset State, Expect Command
      last_tags = false;
    })
  }
  return res.code(200).send('{"took":0, "errors": false }')
}

module.exports = handler
