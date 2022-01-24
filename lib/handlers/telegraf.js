/* Telegraf Handler */
/*

[[outputs.http]]
  url = "http://cloki:3100/telegraf"
  data_format = "json"
  method = "POST"

*/

async function handler (req, res) {
  if (this.debug) console.log('POST /telegraf')
  if (this.debug) console.log('QUERY: ', req.query)
  if (this.debug) console.log('BODY: ', req.body)
  if (!req.body && !req.body.metrics) {
    console.error('No Request Body!', req)
    return
  }
  if (this.readonly) {
    console.error('Readonly! No push support.')
    res.send(500)
    return
  }
  let streams
  streams = req.body.metrics
  if (!Array.isArray(streams)) streams = [streams]
  /** @type {CLokiClient} */
  const client = await req.client()
  if (!streams) {
    res.send(200)
  }
  if (this.debug) console.log('influx', streams)
  for (const stream of streams) {
    const JSONLabels = stream.tags
    JSONLabels.metric = stream.name

    // Calculate Fingerprint
    const finger = await client.storeLabels(JSONLabels)

    if (stream.fields) {
      for (const entry of Object.keys(stream.fields)) {
        if (
          !entry ||
          !entry.timestamp ||
          (!entry.value || !entry.line)
        ) {
          console.error('no bulkable data', entry)
          continue
        }
        await client.storeLogs(finger, [
          finger,
          stream.timestamp * 1000,
          stream.fields[entry] || 0,
          stream.fields[entry].toString() || ''
        ])
      }
    }
  }
}

module.exports = handler
