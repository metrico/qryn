
/* Elastic Indexing Handler
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.

	POST /<target>/_doc/
	PUT /<target>/_doc/<_id>
	PUT /<target>/_create/<_id>
	POST /<target>/_create/<_id>

*/

const { asyncLogError } = require('../../common')
const stringify = require('../utils').stringify
const DATABASE = require('../db/clickhouse')
const { bulk_labels, bulk, labels } = DATABASE.cache
const { fingerPrint } = require('../utils')
const { readonly } = require('../../common')


async function handler (req, res) {
  req.log.debug('ELASTIC Index Request')
  if (!req.body || !req.params.target) {
    asyncLogError('No Request Body or Target!', req.log)
    return res.code(400).send('{"status":400, "error": { "reason": "No Request Body" } }')
  }
  if (readonly) {
    asyncLogError('Readonly! No push support.', req.log)
    return res.code(400).send('{"status":400, "error": { "reason": "Read Only Mode" } }')
  }

  const docTarget = req.params.target || false
  const docId = req.params.id || false

  let streams
  if (
    req.headers['content-type'] &&
    req.headers['content-type'].indexOf('application/json') > -1
  ) {
    // json body, handle as single node array
    streams = [req.body]
  } else {
    // raw body, handle as ndjson
    streams = req.body.split(/\n/)
  }
  req.log.info({ streams }, 'streams')
  const promises = []
  if (streams) {
    streams.forEach(function (stream) {
      req.log.debug({ stream }, 'ingesting elastic doc')
      let finger = null
      let JSONLabels = {}
      try {
        try {
          JSONLabels.type = 'elastic'
          if (docTarget) JSONLabels._index = docTarget
          if (docId) JSONLabels._id = docId
          JSONLabels = Object.fromEntries(Object.entries(JSONLabels).sort())
        } catch (err) {
          asyncLogError(err, req.log)
          return
        }
        // Calculate Fingerprint
        const strJson = stringify(JSONLabels)
        finger = fingerPrint(strJson)
        // Store Fingerprint
        promises.push(bulk_labels.add([[
          new Date().toISOString().split('T')[0],
          finger,
          strJson,
          JSONLabels.target || ''
        ]]))
        for (const key in JSONLabels) {
          req.log.debug({ key, data: JSONLabels[key] }, 'Storing label')
          labels.add('_LABELS_', key)
          labels.add(key, JSONLabels[key])
        }
      } catch (err) {
        asyncLogError(err, req.log)
      }
      // check if stream is JSON format
      try {
        stream = JSON.parse(stream)
      } catch (err) {
        asyncLogError(err, req.log)
      };
      // Store Elastic Doc Object
      const values = [
        finger,
        BigInt((new Date().getTime() * 1000) + '000'),
        null,
        JSON.stringify(stream) || stream
      ]
      req.log.debug({ finger, values }, 'store')
      promises.push(bulk.add([values]))
    })
  }
  await Promise.all(promises)
  res.header('x-elastic-product', 'Elasticsearch')
  return res.code(200).send('{"took":0, "errors": false }')
}

module.exports = handler
