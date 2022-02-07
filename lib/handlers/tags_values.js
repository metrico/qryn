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

function handler (req, res) {
  req.log.debug(`GET /api/search/tag/${req.params.name}/values`)
  const allValues = this.labels.get(req.params.name)
  const resp = { tagValues: allValues }
  res.send(resp)
};

module.exports = handler
