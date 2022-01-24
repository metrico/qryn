/* Label Handler */
/*
   For retrieving the names of the labels one can query on.
   Responses looks like this:
{
  "values": [
    "instance",
    "job",
    ...
  ]
}
*/

async function handler (req, res) {
  /** @type {CLokiClient} */
  const client = await req.client()
  const allLabels = await client.getLabels()
  const resp = { status: 'success', data: allLabels }
  res.send(resp)
}

module.exports = handler
