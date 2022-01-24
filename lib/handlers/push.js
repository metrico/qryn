/* Push Handler */
/*
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.
    Example of the JSON format:
    {
      "streams": [
          {
              "labels": "{foo=\"bar\"}",
              "entries": [
                  {"ts": "2018-12-18T08:28:06.801064-04:00", "line": "baz"}
                ]
              }
            ]
          }
*/

async function handler (req, res) {
  const self = this
  if (this.debug) console.log('POST /loki/api/v1/push')
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
  /** @type {CLokiClient} */
  const client = await req.client()
  let streams
  if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/json') > -1
  ) {
    streams = req.body.streams
  } else if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/x-protobuf') > -1
  ) {
    // streams = messages.PushRequest.decode(req.body)
    streams = req.body
    if (this.debug) console.log('GOT protoBuf', streams)
  }
  if (!streams) {
    res.code(204).send()
    return
  }
  for (const stream of streams) {
    let JSONLabels = null
    try {
      if (stream.stream) {
        JSONLabels = stream.stream
      } else {
        JSONLabels = self.toJSON(
          stream.labels.replace(/\!?="/g, ':"')
        )
      }
      const finger = await client.storeLabels(JSONLabels)
      if (stream.entries) {
        for (const entry of stream.entries) {
          if (!entry &&
            (!entry.timestamp || !entry.ts) &&
            (!entry.value || !entry.line)) {
            continue
          }
          const values = [
            finger,
            new Date(entry.timestamp || entry.ts).getTime(),
            (typeof entry.value === 'undefined') ? null : entry.value,
            entry.line || ''
          ]
          await client.storeLogs(finger, values)
        }
      } else if (stream.values) {
        for (const value of stream.values) {
          if (!value || !value[0] || (!value[1] && typeof value[2] === 'undefined')) {
            continue
          }
          const values = [
            finger,
            Math.round(value[0] / 1000000), // convert to millieseconds
            (typeof value[2] === 'undefined') ? null : value[2],
            value[1] || ''
          ]
          await client.storeLogs(finger, values)
        }
      }
    } catch (e) {
      console.error(e)
      return
    }
  }
  res.code(204).send()
}

module.exports = handler
