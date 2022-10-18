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

const { Transform } = require('stream')

function handleOne (req, streams, promises) {
  const self = this
  streams.on('data', function (stream) {
    req.log.debug({ stream }, 'ingesting tempo stream')
    stream = stream.value
    promises.push(self.pushZipkin([stream]))
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
  const streams = req.body
  const spans = []
  for (const res of streams.resourceSpans) {
    const resAttrs = res.resource && res.resource.attributes ? res.resource.attributes : []
    for (const scope of res.scopeSpans) {
      scope.spans = scope.spans.map(span => ({
        ...span,
        attributes: [
          ...span.attributes,
          ...resAttrs
        ]
      }))
      spans.push.apply(spans, scope.spans)
    }
  }
  await this.pushOTLP(spans)
  res.code(200).send('OK')
}

module.exports = handler
