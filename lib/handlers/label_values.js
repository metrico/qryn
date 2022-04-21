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
const { DATABASE_NAME } = require('../utils')

async function handler (req, res) {
  req.log.debug(`GET /api/prom/label/${req.params.name}/values`)
  const name = req.params.name
  let addValues = []
  if (name === 'db' || name === 'table' || name.substr(0, 2) === '--') {
    let _name = name
    if (name.substr(0, 2) !== '--') {
      _name = '--' + name
    }
    addValues = await getDashedLabel(_name)
    if (name.substr(0, 2) === '--') {
      res.send({ status: 'success', data: addValues })
      return
    }
  }
  let where = [
    `key = ${Sql.val(name)}`,
    req.query.start && !isNaN(parseInt(req.query.start)) ? `date >= toDate(FROM_UNIXTIME(intDiv(${parseInt(req.query.start)}, 1000000000)))` : null,
    req.query.end && !isNaN(parseInt(req.query.end)) ? `date <= toDate(FROM_UNIXTIME(intDiv(${parseInt(req.query.end)}, 1000000000)))` : null
  ].filter(w => w)
  where = `WHERE ${where.join(' AND ')}`
  const q = `SELECT DISTINCT val FROM time_series_gin ${where} FORMAT JSON`
  const allValues = await clickhouse.rawRequest(q, null, utils.DATABASE_NAME())
  const vals = allValues.data.data.map(r => r.val) || []
  vals.push(...addValues)
  const resp = { status: 'success', data: vals }
  res.send(resp)
}

async function getDashedLabel (name) {
  if (name === '--db') {
    const allDBs = await clickhouse.rawRequest('SHOW DATABASES FORMAT JSON')
    return allDBs.data.data.map(db => db.name)
  }
  if (name === '--table') {
    const allDBs = await clickhouse.rawRequest(`SHOW TABLES FROM ${DATABASE_NAME()} FORMAT JSON`)
    return allDBs.data.data.map(db => db.name)
  }
  return []
}

module.exports = handler
