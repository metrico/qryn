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

const clickhouse = require('../db/clickhouse')
const Sql = require('@cloki/clickhouse-sql')
const utils = require('../utils')

async function handler (req, res) {
  req.log.debug(`GET /api/prom/label/${req.params.name}/values`)
  let where = [
    `key = ${Sql.val(req.params.name)}`,
    null, //req.query.start && !isNaN(parseInt(req.query.start)) ? `date >= toDate(FROM_UNIXTIME(intDiv(${parseInt(req.query.start)}, 1000000000)))` : null,
    null, // req.query.end && !isNaN(parseInt(req.query.end)) ? `date <= toDate(FROM_UNIXTIME(intDiv(${parseInt(req.query.end)}, 1000000000)))` : null
  ].filter(w => w)
  where = `WHERE ${where.join(' AND ')}`
  const q = `SELECT DISTINCT val FROM time_series_gin ${where} FORMAT JSON`
  const allValues = await clickhouse.rawRequest(q, null, utils.DATABASE_NAME())
  const resp = { status: 'success', data: allValues.data.data.map(r => r.val) }
  res.send(resp)
}

module.exports = handler
