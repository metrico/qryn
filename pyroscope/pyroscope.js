const messages = require('./querier_pb')
const types = require('./types/v1/types_pb')
const services = require('./querier_grpc_pb')
const clickhouse = require('../lib/db/clickhouse')
const { DATABASE_NAME } = require('../lib/utils')
const Sql = require('@cloki/clickhouse-sql')
const { pyroscopeSelectMergeStacktraces } = require('../wasm_parts/main')
const compiler = require('../parser/bnf')
const { createFlameGraph, readULeb32 } = require('./pprof')
const pprofBin = require('./pprof-bin/pkg/pprof_bin')

const profileTypesHandler = async (req, res) => {
  const _res = new messages.ProfileTypesResponse()
  const fromTimeSec = req.body && req.body.getStart
    ? parseInt(req.body.getStart()) / 1000
    : (Date.now() - 1000 * 60 * 60 * 48) / 1000
  const toTimeSec = req.body && req.body.getEnd
    ? parseInt(req.body.getEnd()) / 1000
    : Date.now() / 1000
  const profileTypes = await clickhouse.rawRequest(`SELECT DISTINCT type_id, sample_type_unit 
FROM profiles_series ARRAY JOIN sample_types_units as sample_type_unit
WHERE date >= toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)})) AND date <= toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)})) FORMAT JSON`,
  null, DATABASE_NAME())
  _res.setProfileTypesList(profileTypes.data.data.map(profileType => {
    const pt = new types.ProfileType()
    const [name, periodType, periodUnit] = profileType.type_id.split(':')
    pt.setId(profileType.type_id + ':' + profileType.sample_type_unit[0] + ':' + profileType.sample_type_unit[1])
    pt.setName(name)
    pt.setSampleType(profileType.sample_type_unit[0])
    pt.setSampleUnit(profileType.sample_type_unit[1])
    pt.setPeriodType(periodType)
    pt.setPeriodUnit(periodUnit)
    return pt
  }))
  return res.code(200).send(Buffer.from(_res.serializeBinary()))
}

const labelNames = async (req, res) => {
  const fromTimeSec = req.body && req.body.getStart
    ? parseInt(req.body.getStart()) / 1000
    : (Date.now() - 1000 * 60 * 60 * 48) / 1000
  const toTimeSec = req.body && req.body.getEnd
    ? parseInt(req.body.getEnd()) / 1000
    : Date.now() / 1000
  const labelNames = await clickhouse.rawRequest(`SELECT DISTINCT key 
FROM profiles_series_keys
WHERE date >= toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)})) AND date <= toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)})) FORMAT JSON`,
  null, DATABASE_NAME())
  const resp = new types.LabelNamesResponse()
  resp.setNamesList(labelNames.data.data.map(label => label.key))
  return res.code(200).send(Buffer.from(resp.serializeBinary()))
}

const labelValues = async (req, res) => {
  const name = req.body && req.body.getName
    ? req.body.getName()
    : ''
  const fromTimeSec = req.body && req.body.getStart && req.body.getStart()
    ? parseInt(req.body.getStart()) / 1000
    : (Date.now() - 1000 * 60 * 60 * 48) / 1000
  const toTimeSec = req.body && req.body.getEnd && req.body.getEnd()
    ? parseInt(req.body.getEnd()) / 1000
    : Date.now() / 1000
  if (!name) {
    throw new Error('No name provided')
  }
  const labelValues = await clickhouse.rawRequest(`SELECT DISTINCT val
FROM profiles_series_gin
WHERE key = ${Sql.quoteVal(name)} AND 
date >= toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)})) AND 
date <= toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)})) FORMAT JSON`, null, DATABASE_NAME())
  const resp = new types.LabelValuesResponse()
  resp.setNamesList(labelValues.data.data.map(label => label.val))
  return res.code(200).send(Buffer.from(resp.serializeBinary()))
}

const parser = (MsgClass) => {
  return async (req, payload) => {
    const _body = []
    payload.on('data', data => {
      _body.push(data)// += data.toString()
    })
    if (payload.isPaused && payload.isPaused()) {
      payload.resume()
    }
    await new Promise(resolve => {
      payload.on('end', resolve)
      payload.on('close', resolve)
    })
    const body = Buffer.concat(_body)
    if (body.length === 0) {
      return null
    }
    req._rawBody = body
    return MsgClass.deserializeBinary(body)
  }
}

let ctxIdx = 0

const selectMergeStacktraces = async (req, res) => {
  const typeRe = req.body.getProfileTypeid().match(/^(.+):([^:]+):([^:]+)$/)
  const sel = req.body.getLabelSelector()
  const fromTimeSec = req.body && req.body.getStart()
    ? Math.floor(parseInt(req.body.getStart()) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  const toTimeSec = req.body && req.body.getEnd()
    ? Math.floor(parseInt(req.body.getEnd()) / 1000)
    : Math.floor(Date.now() / 1000)
  const query = sel ? compiler.ParseScript(sel).rootToken : null
  const idxSelect = (new Sql.Select())
    .select('fingerprint')
    .from('profiles_series_gin')
    .where(
      Sql.And(
        Sql.Eq(new Sql.Raw(`has(sample_types_units, (${Sql.quoteVal(typeRe[2])},${Sql.quoteVal(typeRe[3])}))`), 1),
        Sql.Eq('type_id', Sql.val(typeRe[1])),
        Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
        Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`))
      )
    ).groupBy('fingerprint')
  if (query) {
    const labelsConds = []
    for (const rule of query.Children('log_stream_selector_rule')) {
      const val = JSON.parse(rule.Child('quoted_str').value)
      const labelSubCond = Sql.And(
        Sql.Eq('key', Sql.val(rule.Child('label').value)),
        Sql.Eq('val', Sql.val(val))
      )
      labelsConds.push(labelSubCond)
    }
    idxSelect.where(Sql.Or(...labelsConds))
    idxSelect.having(Sql.Eq(
      new Sql.Raw(labelsConds.map((cond, i) => {
        return `bitShiftLeft(toUInt64(${cond}), ${i})`
      }).join('+')),
      new Sql.Raw(`bitShiftLeft(toUInt64(1), ${labelsConds.length})-1`)
    ))
  }
  const sqlReq = (new Sql.Select())
    .select('payload')
    .from('profiles')
    .where(
      Sql.And(
        Sql.Gte('timestamp_ns', new Sql.Raw(Math.floor(fromTimeSec) + '000000000')),
        Sql.Lte('timestamp_ns', new Sql.Raw(Math.floor(toTimeSec) + '000000000')),
        new Sql.In('fingerprint', 'IN', idxSelect)
      ))
  const profiles = await clickhouse.rawRequest(sqlReq.toString() + 'FORMAT RowBinary', null, DATABASE_NAME(), {
    responseType: 'arraybuffer'
  })
  const binData = Uint8Array.from(profiles.data)
  require('./pprof-bin/pkg/pprof_bin').init_panic_hook()
  const promises = []
  const _ctxIdx = ++ctxIdx
  for (let i = 0; i < binData.length;) {
    const [size, shift] = readULeb32(binData, i)
    const uarray = Uint8Array.from(profiles.data.slice(i + shift, i + size + shift))
    i += size + shift
    promises.push(new Promise((resolve, reject) => setTimeout(() => {
      try {
        pprofBin.merge_tree(_ctxIdx, uarray, `${typeRe[2]}:${typeRe[3]}`)
        resolve()
      } catch (e) {
        reject(e)
      }
    }, 0)))
  }
  let sResp = null
  try {
    await Promise.all(promises)
    sResp = pprofBin.export_tree(_ctxIdx)
  } finally {
    try { pprofBin.drop_tree(_ctxIdx) } catch (e) { req.log.error(e) }
  }
  return res.code(200).send(Buffer.from(sResp))
}

const selectSeries = (req, res) => {
  const resp = new messages.SelectSeriesResponse()
  resp.setSeriesList([])
  return res.code(200).send(Buffer.from(resp.serializeBinary()))
}

module.exports.init = (fastify) => {
  const fns = {
    profileTypes: profileTypesHandler,
    labelNames: labelNames,
    labelValues: labelValues,
    selectMergeStacktraces: selectMergeStacktraces,
    selectSeries: selectSeries
  }
  for (const name of Object.keys(fns)) {
    fastify.post(services.QuerierServiceService[name].path, (req, res) => {
      return fns[name](req, res)
    }, {
      '*': parser(services.QuerierServiceService[name].requestType)
    })
  }
}
