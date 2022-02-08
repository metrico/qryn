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

function handler (req, res) {
  req.log.debug('GET /loki/api/v1/label')
  const allLabels = this.labels.get('_LABELS_')
  const resp = { status: 'success', data: allLabels }
  res.send(resp)
}

module.exports = handler
