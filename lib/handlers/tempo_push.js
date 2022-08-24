/* Zipkin Push Handler
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.
    Example of the Zipkin span JSON format:
    [{
	 "id": "1234",
	 "traceId": "0123456789abcdef",
	 "timestamp": 1608239395286533,
	 "duration": 100000,
	 "name": "span from bash!",
	 "tags": {
		"http.method": "GET",
		"http.path": "/api"
	  },
	  "localEndpoint": {
		"serviceName": "shell script"
	  }
	}]
*/

const stringify = require('../utils').stringify
const { Transform } = require('stream')

function handleOne (req, streams, promises) {
  const self = this
  streams.on('data', function (stream) {
    req.log.debug({ stream }, 'ingesting tempo stream')
    stream = stream.value
    let finger = null
    let JSONLabels = {}
    try {
      try {
        JSONLabels.type = 'tempo'
        if (this.tempo_tagtrace) JSONLabels.traceId = stream.traceId
        if (stream.parentId) JSONLabels.parentId = stream.parentId
        if (stream.name) JSONLabels.name = stream.name
        if (stream.localEndpoint) {
          for (const key in stream.localEndpoint) {
            if (key.includes('.')) {
		    // Convert dot.notation to camelCase
		    var tag = key.split('.').reduce((a, b) => a + b.charAt(0).toUpperCase() + b.slice(1));
		    JSONLabels[tag] = stream.localEndpoint[key]
	    } else {
		    JSONLabels[key] = stream.localEndpoint[key]
	    }
          }
        }
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
        JSONLabels.traceId || ''
      ]])
      self.labels.add(finger.toString(), stream.labels)
      for (const key in JSONLabels) {
        req.log.debug({ key, data: JSONLabels[key] }, 'Storing label')
        self.labels.add('_LABELS_', key)
        self.labels.add(key, JSONLabels[key])
      }
    } catch (err) {
      req.log.error({ err }, 'failed ingesting tempo stream')
    }

    // Form Tempo Object - or shall we materialize this from CH?
    try {
      var tags = Object.keys(stream.tags || {}).map(function (key) {
        return {
          key: key,
          value: typeof stream.tags[key] === 'string'
            ? stream.tags[key]
            : parseInt(stream.tags[key]),
          type: typeof stream.tags[key] === 'string' ? 'string' : 'int64'
        }
      })
      var tempo = {
        traceID: stream.traceId,
        spanID: stream.id,
        name: stream.name,
        references: stream.references || [],
        startTime: stream.timestamp,
        startTimeUnixNano: parseInt(stream.timestamp * 1000),
        endTimeUnixNano: parseInt(stream.timestamp * 1000) + parseInt(stream.duration * 1000 || 1000),
        duration: parseInt(stream.duration) || 1000,
        tags: tags || [],
        logs: [],
        processID: stream.processID || 'p1',
        warnings: null
      }
      if (stream.localEndpoint) tempo.localEndpoint = stream.localEndpoint // already in tags, deduplicate?
      if (stream.parentId) tempo.parentSpanID = stream.parentId
      req.log.debug(tempo)
      tempo = JSON.parse(JSON.stringify(tempo))
    } catch (err) {
      req.log.error({ err })
      return
    }

    // Store Tempo Object
    const values = [
      finger,
      BigInt((stream.timestamp || new Date().getTime() * 1000) + '000'),
      parseInt(stream.duration) || null,
      JSON.stringify(tempo) || ''
    ]
    req.log.debug(`store span: ${tempo.traceID} - ${tempo.spanID}`)
    req.log.debug({ finger, values }, 'store')
    promises.push(self.bulk.add([values]))
  })
}

async function handler (req, res) {
  req.log.debug('POST /tempo/api/push')
  if (!req.body) {
    req.log.error('No Request Body!')
    res.code(500).send()
    return
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    res.code(500).send()
    return
  }
  let streams = req.body
  if (
    req.headers['content-type'] &&
    req.headers['content-type'].indexOf('application/x-protobuf') > -1
  ) {
    streams = new Transform({
      transform (chunk, encoding, callback) {
        callback(chunk)
      }
    })
    const sendStreams = (async () => {
      for (const s of req.body) {
        while (!streams.write(s)) {
          await new Promise(resolve => streams.once('drain', resolve))
        }
      }
    })()
    handleOne.bind(this)(req, streams)
    await sendStreams
    req.log.debug({ streams }, 'GOT protoBuf')
  } else {
    streams = req.body
    if (req.body.error) {
      throw req.body.error
    }
    const promises = []
    handleOne.bind(this)(req, streams, promises)
    await new Promise((resolve, reject) => {
      req.body.once('close', resolve)
      req.body.once('end', resolve)
      req.body.once('error', reject)
    })
    req.log.debug(`waiting for ${promises.length} promises`)
    await Promise.all(promises)
  }

  res.code(200).send('OK')
}

module.exports = handler
