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
async function handler (req, res) {
  const self = this
  if (this.debug) console.log('POST /tempo/api/push')
  if (this.debug) console.log('QUERY: ', req.query)
  if (this.debug) console.log('BODY: ', req.body)
  if (!req.body) {
    console.error('No Request Body!', req)
    res.code(500).send()
    return
  }
  if (this.readonly) {
    console.error('Readonly! No push support.')
    res.code(500).send()
    return
  }
  let streams
  if (
    req.headers['content-type'] &&
    req.headers['content-type'].indexOf('application/json') > -1
  ) {
    streams = req.body
  } else if (
    req.headers['content-type'] &&
    req.headers['content-type'].indexOf('application/x-protobuf') > -1
  ) {
    streams = req.body
    if (this.debug) console.log('GOT protoBuf', streams)
  } else {
    streams = req.body
  }
  console.log('streams', streams)
  if (!streams) {
    res.code(204).send()
    return
  }
  /** @type {CLokiClient} */
  const client = await req.client()

  for (const stream of streams) {
    if (self.debug) console.log('ingesting tempo stream', stream)
    let JSONLabels = {}
    JSONLabels.type = 'tempo'
    if (this.tempo_tagtrace) JSONLabels.traceId = stream.traceId
    if (stream.parentId) JSONLabels.parentId = stream.parentId
    if (stream.localEndpoint) {
      for (const key in stream.localEndpoint) {
        JSONLabels[key] = stream.localEndpoint[key]
      }
    }
    JSONLabels = Object.fromEntries(Object.entries(JSONLabels).sort())
    const finger = await client.storeLabels(JSONLabels)

    // Form Tempo Object - or shall we materialize this from CH?
    const tags = Object.keys(stream.tags || {}).map(function (key) {
      return {
        key: key,
        value: typeof stream.tags[key] === 'string' ? stream.tags[key] : parseInt(stream.tags[key]),
        type: typeof stream.tags[key] === 'string' ? 'string' : 'int64'
      }
    })
    let tempo = {
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
    if (self.debug) console.log(tempo)
    tempo = JSON.parse(JSON.stringify(tempo))
    await client.storeLogs(finger, [
      finger,
      parseInt(stream.timestamp / 1000) || new Date().getTime(),
      parseInt(stream.duration) || null,
      JSON.stringify(tempo) || ''
    ])
  }
  res.code(204).send()
}

module.exports = handler
