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
const { asyncLogError } = require('../../common')

async function handler (req, res) {
  req.log.debug(`GET /api/search/tag/${req.params.name}/values`)
  if (!req.params.name) {
    return res.send({ tagValues: [] })
  }
  try {
    const vals = (await this.queryTempoValues(req.params.name)).map(e => e.val)
    return res.send({ tagValues: vals })
  } catch (e) {
    asyncLogError(e, req.log)
    res.code(500)
  }
};

module.exports = handler
