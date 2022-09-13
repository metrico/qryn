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
  req.log.debug(`GET /api/search/tag/${req.params.name}/values`)
  if (!req.params.name) {
    res.send({ tagValues: [] })
  }
  try {
    const vals = (await this.queryTempoValues(req.params.name)).map(e => e.val)
    res.send({ tagValues: vals })
  } catch (e) {
    req.log.error(e)
    res.code(500)
  }
};

module.exports = handler
