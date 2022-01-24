/* Tags Label Handler */
/*
   For retrieving the names of the tags tempo can query on.
   Responses looks like this:
{
  "tagNames": [
    "instance",
    "job",
    ...
  ]
}
*/

async function handler (req, res) {
  if (this.debug) console.log('GET /api/search/tags')
  if (this.debug) console.log('QUERY: ', req.query)
  /** @type {CLokiClient} */
  const client = await req.client()
  const allLabels = await client.getLabels()
  const resp = { tagNames: allLabels }
  res.send(resp)
};

module.exports = handler
