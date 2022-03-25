const clickhouse = require('./clickhouse')
const transpiler = require('../../parser/transpiler')
const Sql = require('@cloki/clickhouse-sql')
const { sharedParamNames } = require('../../parser/registry/common')
const CORS = process.env.CORS_ALLOW_ORIGIN || '*'

const queryRange = async (query, res) => {
  const sql = transpiler.transpile({ ...query, rawQuery: true })
  const strSel = sql.query.withs.str_sel.query
  strSel.select_list = []
  strSel.select(['fingerprint', 'fingerprint'])

  const samplesTable = sql.query.params[sharedParamNames.samplesTable]
  const timeSeriesTable = sql.query.params[sharedParamNames.timeSeriesTable]
  const from = sql.query.params[sharedParamNames.from]
  const to = sql.query.params[sharedParamNames.to]
  const _from = from.get()
  let limit = sql.query.params[sharedParamNames.limit].get()
  limit = limit ? parseInt(limit) : 0
  let head = false
  let i = 0
  while (true) {
    const tsClause = new Sql.Raw('')
    tsClause.toString = () => {
      if (to.get()) {
        return Sql.between('samples.timestamp_ns', from, to).toString()
      }
      return Sql.Gt('samples.timestamp_ns', from).toString()
    }
    const thsndFPSelect = (new Sql.Select())
      .select('timestamp_ns', 'fingerprint')
      .from([samplesTable, 'samples'])
      .where(Sql.And(tsClause, Sql.in('fingerprint', strSel)))
      .limit(1000)
      .orderBy(['timestamp_ns', 'desc'])
      .format('JSON')
    if (!head) {
      res.res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': CORS })
      res.res.write('{"status":"success", "data":{ "resultType": "streams", "result": [')
      head = true
    }
    const fpsResp = await clickhouse.rawRequest(thsndFPSelect.toString())
    const fps = new Set()
    let max = 0
    let min = 0
    if (!fpsResp.data.data.length) {
      break
    }

    for (const row of fpsResp.data.data) {
      fps.add(row.fingerprint)
      if (!max) max = row.timestamp_ns
      min = row.timestamp_ns
    }
    min = min !== max ? BigInt(min) + BigInt(1) : BigInt(min)
    from.set(min)
    to.set(max)
    sql.query.withs.str_sel.query = (new Sql.Select())
      .select('fingerprint', 'labels')
      .from(timeSeriesTable)
      .where(Sql.in('fingerprint', Array.from(fps)))
    let stream = await clickhouse.getClickhouseStream({ query: sql.query.toString() })

    stream = clickhouse.preprocessStream(stream, [(s) => s.map(function (row) {
      if (row && row.labels) {
        i++
        if (limit && i > limit) {
          return undefined
        }
      }
      return row
    })])
    if (limit && i > limit) {
      break
    }
    await clickhouse.outputQueryStreams(stream, {
      res: {
        writeHead: () => {},
        write: (msg) => res.res.write(msg),
        onBegin: () => {},
        onEnd: () => {},
        end: () => {}
      }
    }, i)
    from.set(_from)
    to.set((min - BigInt(1)).toString())
  }
  res.res.write(']}}')
  res.res.end()
}

module.exports = {
  queryRange
}
