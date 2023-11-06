/* Tag Value Handler V2 */
/*
   For retrieving the tag values tempo can query on.
   Responses looks like this:
{
  "tagValues": [
    {
      "type": "string",
      "value": "a"
    },
    ....
  ]
}
*/
const { asyncLogError } = require('../../common')
const { queryTempoValues } = require('../db/clickhouse')

async function handler (req, res) {
  req.log.debug(`GET /api/v2/search/tag/${req.params.name}/values`)
  if (!req.params.name) {
    return res.send({ tagValues: [] })
  }
  try {
    req.params.name = req.params.name.replace(/^resource\.|^span\./, '')
    const vals = (await queryTempoValues(req.params.name)).map(e => e.val)
    return res.send({ tagValues: vals.map(v => ({ type: 'string', value: v })) })
  } catch (e) {
    asyncLogError(e, req.log)
    res.code(500)
  }
};

module.exports = handler
