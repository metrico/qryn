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
const { asyncLogError } = require('../../common')

function handleOne (req, streams, promises) {
  const self = this
  streams.on('data', function (stream) {
    stream = stream.value
    promises.push(self.pushZipkin([stream]))
  })
}

async function handler (req, res) {
  req.log.debug('POST /tempo/api/push')
  if (!req.body) {
    asyncLogError('No Request Body!', req.log)
    res.code(500).send()
    return
  }
  if (this.readonly) {
    asyncLogError('Readonly! No push support.', req.log)
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
