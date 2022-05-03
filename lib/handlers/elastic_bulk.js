
/* Elastic Indexing Handler
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.

	POST /_bulk
	POST /<target>/_bulk

*/

const stringify = require('json-stable-stringify')

function handler (req, res) {
  const self = this
  req.log.debug('ELASTIC Bulk Request')
  if (!req.body || !req.params.target) {
    req.log.error('No Request Body or Target!')
    res.code(500).send()
    return
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    res.code(500).send()
    return
  }

  const doc_target = req.params.target || false;

  let streams
  if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/x-ndjson') > -1
  ) {
    // ndjson body
    streams = req.body.split(/\r?\n/)
  } else {
    // assume ndjson raw body
    streams = req.body.split(/\r?\n/)
  }
  req.log.debug({ streams}, streams.lenght + ' bulk streams')
  req.log.debug({ streams }, 'streams')
  var last_tags = false;
  if (streams) {
    streams.forEach(function (stream) {
      req.log.debug({ stream }, 'ingesting elastic bulk row')

      try {
        stream = JSON.parse(JSON.stringify(stream))
      } catch (err) { req.log.error({ err }); return };

      // Allow Index, Create. Discard Delete, Update.
      if (index.delete||index.update) { last_tags = false; return; }
      var command = stream.index || stream.create || false;
      if (command && !last_tags) {
	last_tags=stream.index;
	return;
      }

      // Data Row
      let finger = null
      let JSONLabels = last_tags;
      try {
        try {
          JSONLabels.type = 'elastic'
          if (doc_target) JSONLabels._index = doc_target
          JSONLabels = Object.fromEntries(Object.entries(JSONLabels).sort())
        } catch (err) {
          req.log.error({ err })
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
        req.log.error({ err }, 'failed ingesting elastic doc')
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
  res.code(200).send()
}

module.exports = handler
