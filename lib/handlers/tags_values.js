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

const DATABASE = require('../db/clickhouse')
const { labels } = DATABASE.cache

function handler (req, res) {
  req.log.debug(`GET /api/search/tag/${req.params.name}/values`)
  if (req.params.name.includes('.')) {
    var tag = req.params.name.split('.').reduce((a, b) => a + b.charAt(0).toUpperCase() + b.slice(1));
    const allValues = labels.get(tag)
    const resp = { tagValues: allValues }
    return res.send(resp)
  } else {
    const allValues = labels.get(req.params.name)
    const resp = { tagValues: allValues }
    return res.send(resp)
  }
};

module.exports = handler
