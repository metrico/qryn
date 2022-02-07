/* Label Value Handler */
/*
   For retrieving the label values one can query on.
   Responses looks like this:
  {
  "values": [
    "default",
    "cortex-ops",
    ...
  ]
}
*/

function handler (req, res) {
  req.log.debug(`GET /api/prom/label/${req.params.name}/values`)
  req.log.debug('QUERY LABEL: %s', req.params.name)
  const allValues = this.labels.get(req.params.name)
  const resp = { status: 'success', data: allValues }
  res.send(resp)
};

module.exports = handler
