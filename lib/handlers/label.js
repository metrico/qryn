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

const clickhouse = require('../db/clickhouse')
const utils = require('../utils')
const { clusterName } = require('../../common')
const dist = clusterName ? '_dist' : ''

async function handler (req, res) {
  req.log.debug('GET /loki/api/v1/label')
  let where = [
    req.query.start && !isNaN(parseInt(req.query.start)) ? `date >= toDate(FROM_UNIXTIME(intDiv(${parseInt(req.query.start)}, 1000000000)))` : null,
    req.query.end && !isNaN(parseInt(req.query.end)) ? `date <= toDate(FROM_UNIXTIME(intDiv(${parseInt(req.query.end)}, 1000000000)))` : null
  ].filter(w => w)
  where = where.length ? `WHERE ${where.join(' AND ')}` : ''
  const q = `SELECT DISTINCT key FROM time_series_gin${dist} ${where} FORMAT JSON`
  console.log(q)
  const allLabels = await clickhouse.rawRequest(q, null, utils.DATABASE_NAME())
  const resp = { status: 'success', data: allLabels.data.data.map(r => r.key) }
  return res.send(resp)
}

module.exports = handler
