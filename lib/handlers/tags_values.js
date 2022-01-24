/* Tag Value Handler */
/*
   For retrieving the tag values tempo can query on.
   Responses looks like this:
  {
  "tagValues": [
    "default",
    "cortex-ops",
    ...
  ]
}
*/

async function handler (req, res) {
  if (this.debug) { console.log('GET /api/search/tag/' + req.params.name + '/values') }
  if (this.debug) console.log('QUERY LABEL: ', req.params.name)
  /** @type {CLokiClient} */
  const client = await req.client()
  const allValues = await client.getLabelValues(req.params.name)
  const resp = { tagValues: allValues }
  res.send(resp)
}

module.exports = handler
