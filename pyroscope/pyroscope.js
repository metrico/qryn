const messages = require('./querier_pb')
const types = require('./types/v1/types_pb')
const services = require('./querier_grpc_pb')
const clickhouse = require('../lib/db/clickhouse')
const { DATABASE_NAME, checkVersion } = require('../lib/utils')
const Sql = require('@cloki/clickhouse-sql')
const compiler = require('../parser/bnf')
const { readULeb32 } = require('./pprof')
const pprofBin = require('./pprof-bin/pkg/pprof_bin')
const { QrynBadRequest } = require('../lib/handlers/errors')
const { clusterName } = require('../common')
const logger = require('../lib/logger')

const HISTORY_TIMESPAN = 1000 * 60 * 60 * 24 * 7

/**
 *
 * @param typeId {string}
 */
const parseTypeId = (typeId) => {
  const typeParts = typeId.match(/^([^:]+):([^:]+):([^:]+):([^:]+):([^:]+)$/)
  if (!typeParts) {
    throw new QrynBadRequest('invalid type id')
  }
  return {
    type: typeParts[1],
    sampleType: typeParts[2],
    sampleUnit: typeParts[3],
    periodType: typeParts[4],
    periodUnit: typeParts[5]
  }
}

const profileTypesHandler = async (req, res) => {
  const dist = clusterName ? '_dist' : ''
  const _res = new messages.ProfileTypesResponse()
  const fromTimeSec = req.body && req.body.getStart
    ? parseInt(req.body.getStart()) / 1000
    : (Date.now() - HISTORY_TIMESPAN) / 1000
  const toTimeSec = req.body && req.body.getEnd
    ? parseInt(req.body.getEnd()) / 1000
    : Date.now() / 1000
  const profileTypes = await clickhouse.rawRequest(`SELECT DISTINCT type_id, sample_type_unit 
FROM profiles_series${dist} ARRAY JOIN sample_types_units as sample_type_unit
WHERE date >= toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)})) AND date <= toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)})) FORMAT JSON`,
  null, DATABASE_NAME())
  _res.setProfileTypesList(profileTypes.data.data.map(profileType => {
    const pt = new types.ProfileType()
    const [name, periodType, periodUnit] = profileType.type_id.split(':')
    const typeIdParts = profileType.type_id.match(/^([^:]+):(.*)$/)
    pt.setId(typeIdParts[1] + ':' + profileType.sample_type_unit[0] + ':' + profileType.sample_type_unit[1] +
      ':' + typeIdParts[2])
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
  const dist = clusterName ? '_dist' : ''
  const fromTimeSec = req.body && req.body.getStart
    ? parseInt(req.body.getStart()) / 1000
    : (Date.now() - HISTORY_TIMESPAN) / 1000
  const toTimeSec = req.body && req.body.getEnd
    ? parseInt(req.body.getEnd()) / 1000
    : Date.now() / 1000
  const labelNames = await clickhouse.rawRequest(`SELECT DISTINCT key 
FROM profiles_series_keys${dist}
WHERE date >= toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)})) AND date <= toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)})) FORMAT JSON`,
  null, DATABASE_NAME())
  const resp = new types.LabelNamesResponse()
  resp.setNamesList(labelNames.data.data.map(label => label.key))
  return res.code(200).send(Buffer.from(resp.serializeBinary()))
}

const labelValues = async (req, res) => {
  const dist = clusterName ? '_dist' : ''
  const name = req.body && req.body.getName
    ? req.body.getName()
    : ''
  const fromTimeSec = req.body && req.body.getStart && req.body.getStart()
    ? parseInt(req.body.getStart()) / 1000
    : (Date.now() - HISTORY_TIMESPAN) / 1000
  const toTimeSec = req.body && req.body.getEnd && req.body.getEnd()
    ? parseInt(req.body.getEnd()) / 1000
    : Date.now() / 1000
  if (!name) {
    throw new Error('No name provided')
  }
  const labelValues = await clickhouse.rawRequest(`SELECT DISTINCT val
FROM profiles_series_gin${dist}
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

/**
 *
 * @param {Sql.Select} query
 * @param {string} labelSelector
 */
const labelSelectorQuery = (query, labelSelector) => {
  if (!labelSelector || !labelSelector.length || labelSelector === '{}') {
    return query
  }
  const labelSelectorScript = compiler.ParseScript(labelSelector).rootToken
  const labelsConds = []
  for (const rule of labelSelectorScript.Children('log_stream_selector_rule')) {
    const val = JSON.parse(rule.Child('quoted_str').value)
    let valRul = null
    switch (rule.Child('operator').value) {
      case '=':
        valRul = Sql.Eq(new Sql.Raw('val'), Sql.val(val))
        break
      case '!=':
        valRul = Sql.Ne(new Sql.Raw('val'), Sql.val(val))
        break
      case '=~':
        valRul = Sql.Eq(new Sql.Raw(`match(val, ${Sql.quoteVal(val)})`), 1)
        break
      case '!~':
        valRul = Sql.Ne(new Sql.Raw(`match(val, ${Sql.quoteVal(val)})`), 1)
    }
    const labelSubCond = Sql.And(
      Sql.Eq('key', Sql.val(rule.Child('label').value)),
      valRul
    )
    labelsConds.push(labelSubCond)
  }
  query.where(Sql.Or(...labelsConds))
  query.groupBy(new Sql.Raw('fingerprint'))
  query.having(Sql.Eq(
    new Sql.Raw(`groupBitOr(${labelsConds.map((cond, i) => {
      return `bitShiftLeft(toUInt64(${cond}), ${i})`
    }).join('+')})`),
    new Sql.Raw(`bitShiftLeft(toUInt64(1), ${labelsConds.length})-1`)
  ))
}

const serviceNameSelectorQuery = (labelSelector) => {
  const empty = Sql.Eq(new Sql.Raw('1'), new Sql.Raw('1'))
  if (!labelSelector || !labelSelector.length || labelSelector === '{}') {
    return empty
  }
  const labelSelectorScript = compiler.ParseScript(labelSelector).rootToken
  let conds = null
  for (const rule of labelSelectorScript.Children('log_stream_selector_rule')) {
    const label = rule.Child('label').value
    if (label !== 'service_name') {
      continue
    }
    const val = JSON.parse(rule.Child('quoted_str').value)
    let valRul = null
    switch (rule.Child('operator').value) {
      case '=':
        valRul = Sql.Eq(new Sql.Raw('service_name'), Sql.val(val))
        break
      case '!=':
        valRul = Sql.Ne(new Sql.Raw('service_name'), Sql.val(val))
        break
      case '=~':
        valRul = Sql.Eq(new Sql.Raw(`match(service_name, ${Sql.quoteVal(val)})`), 1)
        break
      case '!~':
        valRul = Sql.Ne(new Sql.Raw(`match(service_name, ${Sql.quoteVal(val)})`), 1)
    }
    conds = valRul
  }
  return conds || empty
}

const selectMergeStacktraces = async (req, res) => {
  return await selectMergeStacktracesV2(req, res)
}

const sqlWithReference = (ref) => {
  const res = new Sql.WithReference(ref)
  res.toString = function () {
    if (this.ref.inline) {
      return `(${this.ref.query.toString()}) as ${this.ref.alias}`
    }
    return this.ref.alias
  }
  return res
}

const selectMergeStacktracesV2 = async (req, res) => {
  const dist = clusterName ? '_dist' : ''
  const typeRegex = parseTypeId(req.body.getProfileTypeid())
  const sel = req.body.getLabelSelector()
  const fromTimeSec = req.body && req.body.getStart()
    ? Math.floor(parseInt(req.body.getStart()) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  const toTimeSec = req.body && req.body.getEnd()
    ? Math.floor(parseInt(req.body.getEnd()) / 1000)
    : Math.floor(Date.now() / 1000)
  const v2 = checkVersion('profiles_v2', (fromTimeSec - 3600) * 1000)
  const serviceNameSelector = serviceNameSelectorQuery(sel)
  const typeIdSelector = Sql.Eq(
    'type_id',
    Sql.val(`${typeRegex.type}:${typeRegex.periodType}:${typeRegex.periodUnit}`)
  )
  const idxSelect = (new Sql.Select())
    .select('fingerprint')
    .from(`${DATABASE_NAME()}.profiles_series_gin`)
    .where(
      Sql.And(
        Sql.Eq(new Sql.Raw(`has(sample_types_units, (${Sql.quoteVal(typeRegex.sampleType)},${Sql.quoteVal(typeRegex.sampleUnit)}))`), 1),
        typeIdSelector,
        Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
        Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`)),
        serviceNameSelector
      )
    ).groupBy('fingerprint')
  labelSelectorQuery(idxSelect, sel)
  const withIdxSelect = new Sql.With('idx', idxSelect, !!clusterName)
  const rawReq = (new Sql.Select()).with(withIdxSelect)
    .select([
      new Sql.Raw(`arrayMap(x -> (x.1, x.2, x.3, (arrayFirst(y -> y.1 == ${Sql.quoteVal(`${typeRegex.sampleType}:${typeRegex.sampleUnit}`)}, x.4) as af).2, af.3), tree)`),
      'tree'
    ], 'functions')
    .from(`${DATABASE_NAME()}.profiles${dist}`)
    .where(
      Sql.And(
        Sql.Gte('timestamp_ns', new Sql.Raw(Math.floor(fromTimeSec) + '000000000')),
        Sql.Lte('timestamp_ns', new Sql.Raw(Math.floor(toTimeSec) + '000000000')),
        new Sql.In('fingerprint', 'IN', sqlWithReference(withIdxSelect)),
        typeIdSelector,
        serviceNameSelector
      ))
  if (process.env.ADVANCED_PROFILES_MERGE_LIMIT) {
    rawReq.orderBy(['timestamp_ns', 'desc']).limit(parseInt(process.env.ADVANCED_PROFILES_MERGE_LIMIT))
  }
  const withRawReq = new Sql.With('raw', rawReq, !!clusterName)
  const joinedReq = (new Sql.Select()).with(withRawReq).select([
    new Sql.Raw('(raw.tree.1, raw.tree.2, raw.tree.3, sum(raw.tree.4), sum(raw.tree.5))'),
    'tree2'
  ]).from(sqlWithReference(withRawReq))
    .join('raw.tree', 'array')
    .groupBy(new Sql.Raw('raw.tree.1'), new Sql.Raw('raw.tree.2'), new Sql.Raw('raw.tree.3'))
    .orderBy(new Sql.Raw('raw.tree.1')).limit(2000000)
  const withJoinedReq = new Sql.With('joined', joinedReq, !!clusterName)
  const joinedAggregatedReq = (new Sql.Select()).select(
    [new Sql.Raw('groupArray(tree2)'), 'tree']).from(sqlWithReference(withJoinedReq))
  const functionsReq = (new Sql.Select()).select(
    [new Sql.Raw('groupUniqArray(raw.functions)'), 'functions2']
  ).from(sqlWithReference(withRawReq)).join('raw.functions', 'array')

  let brackLegacy = (new Sql.Select()).select(
    [new Sql.Raw('[]::Array(String)'), 'legacy']
  )
  let withLegacy = null
  if (!v2) {
    const legacy = (new Sql.Select()).with(withIdxSelect)
      .select('payload')
      .from(`${DATABASE_NAME()}.profiles${dist}`)
      .where(
        Sql.And(
          Sql.Gte('timestamp_ns', new Sql.Raw(Math.floor(fromTimeSec) + '000000000')),
          Sql.Lte('timestamp_ns', new Sql.Raw(Math.floor(toTimeSec) + '000000000')),
          new Sql.In('fingerprint', 'IN', sqlWithReference(withIdxSelect)),
          Sql.Eq(new Sql.Raw('empty(tree)'), 1),
          typeIdSelector,
          serviceNameSelector
        ))
    if (process.env.ADVANCED_PROFILES_MERGE_LIMIT) {
      legacy.orderBy(['timestamp_ns', 'desc']).limit(parseInt(process.env.ADVANCED_PROFILES_MERGE_LIMIT))
    }
    withLegacy = new Sql.With('legacy', legacy, !!clusterName)
    brackLegacy = (new Sql.Select())
      .select([new Sql.Raw('groupArray(payload)'), 'payloads'])
      .from(sqlWithReference(withLegacy))
  }
  brackLegacy = new Sql.Raw(`(${brackLegacy.toString()})`)
  const brack1 = new Sql.Raw(`(${joinedAggregatedReq.toString()})`)
  const brack2 = new Sql.Raw(`(${functionsReq.toString()})`)

  const sqlReq = (new Sql.Select())
    .select(
      [brackLegacy, 'legacy'],
      [brack2, 'functions'],
      [brack1, 'tree']
    )
  if (v2) {
    sqlReq.with(withJoinedReq, withRawReq)
  } else {
    sqlReq.with(withJoinedReq, withRawReq, withLegacy)
  }

  let start = Date.now()
  console.log(sqlReq.toString())
  const profiles = await clickhouse.rawRequest(sqlReq.toString() + ' FORMAT RowBinary',
    null,
    DATABASE_NAME(),
    {
      responseType: 'arraybuffer'
    })
  const binData = Uint8Array.from(profiles.data)
  req.log.debug(`selectMergeStacktraces: profiles downloaded: ${binData.length / 1025}kB in ${Date.now() - start}ms`)
  require('./pprof-bin/pkg/pprof_bin').init_panic_hook()
  const _ctxIdx = ++ctxIdx
  const [legacyLen, shift] = readULeb32(binData, 0)
  let ofs = shift
  try {
    let mergePprofLat = BigInt(0)
    for (let i = 0; i < legacyLen; i++) {
      const [profLen, shift] = readULeb32(binData, ofs)
      ofs += shift
      start = process.hrtime?.bigint ? process.hrtime.bigint() : BigInt(0)
      pprofBin.merge_prof(_ctxIdx,
        Uint8Array.from(profiles.data.slice(ofs, ofs + profLen)),
        `${typeRegex.sampleType}:${typeRegex.sampleUnit}`)
      mergePprofLat += (process.hrtime?.bigint ? process.hrtime.bigint() : BigInt(0)) - start
      ofs += profLen
    }
    start = process.hrtime?.bigint ? process.hrtime.bigint() : BigInt(0)
    pprofBin.merge_tree(_ctxIdx, Uint8Array.from(profiles.data.slice(ofs)),
      typeRegex.sampleType + ':' + typeRegex.sampleUnit)
    const mergeTreeLat = (process.hrtime?.bigint ? process.hrtime.bigint() : BigInt(0)) - start
    start = process.hrtime?.bigint ? process.hrtime.bigint() : BigInt(0)
    const resp = pprofBin.export_tree(_ctxIdx, typeRegex.sampleType + ':' + typeRegex.sampleUnit)
    const exportTreeLat = (process.hrtime?.bigint ? process.hrtime.bigint() : BigInt(0)) - start
    req.log.debug(`merge_pprof: ${mergePprofLat / BigInt(1000000)}ms`)
    req.log.debug(`merge_tree: ${mergeTreeLat / BigInt(1000000)}ms`)
    req.log.debug(`export_tree: ${exportTreeLat / BigInt(1000000)}ms`)
    return res.code(200).send(Buffer.from(resp))
  } finally {
    try { pprofBin.drop_tree(_ctxIdx) } catch (e) {}
  }
}

const selectSeries = async (req, res) => {
  const _req = req.body
  const fromTimeSec = Math.floor(req.getStart && req.getStart()
    ? parseInt(req.getStart()) / 1000
    : Date.now() / 1000 - HISTORY_TIMESPAN)
  const toTimeSec = Math.floor(req.getEnd && req.getEnd()
    ? parseInt(req.getEnd()) / 1000
    : Date.now() / 1000)
  let typeID = _req.getProfileTypeid && _req.getProfileTypeid()
  if (!typeID) {
    throw new QrynBadRequest('No type provided')
  }
  typeID = parseTypeId(typeID)
  if (!typeID) {
    throw new QrynBadRequest('Invalid type provided')
  }
  const dist = clusterName ? '_dist' : ''
  const sampleTypeId = typeID.sampleType + ':' + typeID.sampleUnit
  const labelSelector = _req.getLabelSelector && _req.getLabelSelector()
  let groupBy = _req.getGroupByList && _req.getGroupByList()
  groupBy = groupBy && groupBy.length ? groupBy : null
  const step = _req.getStep && parseInt(_req.getStep())
  if (!step || isNaN(step)) {
    throw new QrynBadRequest('No step provided')
  }
  const aggregation = _req.getAggregation && _req.getAggregation()

  const typeIdSelector = Sql.Eq(
    'type_id',
    Sql.val(`${typeID.type}:${typeID.periodType}:${typeID.periodUnit}`))
  const serviceNameSelector = serviceNameSelectorQuery(labelSelector)

  const idxReq = (new Sql.Select())
    .select(new Sql.Raw('fingerprint'))
    .from(`${DATABASE_NAME()}.profiles_series_gin`)
    .where(
      Sql.And(
        typeIdSelector,
        serviceNameSelector,
        Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
        Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`)),
        Sql.Eq(new Sql.Raw(
          `has(sample_types_units, (${Sql.quoteVal(typeID.sampleType)}, ${Sql.quoteVal(typeID.sampleUnit)}))`),
        1)
      )
    )
  labelSelectorQuery(idxReq, labelSelector)

  const withIdxReq = (new Sql.With('idx', idxReq, !!clusterName))

  let tagsReq = 'arraySort(p.tags)'
  if (groupBy) {
    tagsReq = `arraySort(arrayFilter(x -> x.1 in (${groupBy.map(g => Sql.quoteVal(g)).join(',')}), p.tags))`
  }

  const labelsReq = (new Sql.Select()).with(withIdxReq).select(
    'fingerprint',
    [new Sql.Raw(tagsReq), 'tags'],
    [groupBy ? new Sql.Raw('cityHash64(tags)') : 'fingerprint', 'new_fingerprint']
  ).distinct(true).from([`${DATABASE_NAME()}.profiles_series`, 'p'])
    .where(Sql.And(
      new Sql.In('fingerprint', 'IN', new Sql.WithReference(withIdxReq)),
      Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
      Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`)),
      typeIdSelector,
      serviceNameSelector
    ))

  const withLabelsReq = new Sql.With('labels', labelsReq, !!clusterName)

  let valueCol = new Sql.Raw(
    `sum(toFloat64(arrayFirst(x -> x.1 == ${Sql.quoteVal(sampleTypeId)}, p.values_agg).2))`)
  if (aggregation === types.TimeSeriesAggregationType.TIME_SERIES_AGGREGATION_TYPE_AVERAGE) {
    valueCol = new Sql.Raw(
      `sum(toFloat64(arrayFirst(x -> x.1 == ${Sql.quoteVal(sampleTypeId)}).2, p.values_agg)) / ` +
      `sum(toFloat64(arrayFirst(x -> x.1 == ${Sql.quoteVal(sampleTypeId)}).3, p.values_agg))`
    )
  }

  const mainReq = (new Sql.Select()).with(withIdxReq, withLabelsReq).select(
    [new Sql.Raw(`intDiv(p.timestamp_ns, 1000000000 * ${step}) * ${step} * 1000`), 'timestamp_ms'],
    [new Sql.Raw('labels.new_fingerprint'), 'fingerprint'],
    [new Sql.Raw('min(labels.tags)'), 'labels'],
    [valueCol, 'value']
  ).from([`${DATABASE_NAME()}.profiles${dist}`, 'p']).join(
    [new Sql.WithReference(withLabelsReq), 'labels'],
    'ANY LEFT',
    Sql.Eq(new Sql.Raw('p.fingerprint'), new Sql.Raw('labels.fingerprint'))
  ).where(
    Sql.And(
      new Sql.In('p.fingerprint', 'IN', new Sql.WithReference(withIdxReq)),
      Sql.Gte('p.timestamp_ns', new Sql.Raw(`${fromTimeSec}000000000`)),
      Sql.Lt('p.timestamp_ns', new Sql.Raw(`${toTimeSec}000000000`)),
      typeIdSelector,
      serviceNameSelector
    )
  ).groupBy('timestamp_ns', 'fingerprint')
    .orderBy(['fingerprint', 'ASC'], ['timestamp_ns', 'ASC'])
  const strMainReq = mainReq.toString()
  console.log(strMainReq)
  const chRes = await clickhouse
    .rawRequest(strMainReq + ' FORMAT JSON', null, DATABASE_NAME())

  let lastFingerprint = null
  const seriesList = []
  let lastSeries = null
  let lastPoints = []
  for (let i = 0; i < chRes.data.data.length; i++) {
    const e = chRes.data.data[i]
    if (lastFingerprint !== e.fingerprint) {
      lastFingerprint = e.fingerprint
      lastSeries && lastSeries.setPointsList(lastPoints)
      lastSeries && seriesList.push(lastSeries)
      lastPoints = []
      lastSeries = new types.Series()
      lastSeries.setLabelsList(e.labels.map(l => {
        const lp = new types.LabelPair()
        lp.setName(l[0])
        lp.setValue(l[1])
        return lp
      }))
    }

    const p = new types.Point()
    p.setValue(e.value)
    p.setTimestamp(e.timestamp_ms)
    lastPoints.push(p)
  }
  lastSeries && lastSeries.setPointsList(lastPoints)
  lastSeries && seriesList.push(lastSeries)

  const resp = new messages.SelectSeriesResponse()
  resp.setSeriesList(seriesList)
  return res.code(200).send(Buffer.from(resp.serializeBinary()))
}

const selectMergeProfile = async (req, res) => {
  const _req = req.body
  const fromTimeSec = Math.floor(req.getStart && req.getStart()
    ? parseInt(req.getStart()) / 1000
    : Date.now() / 1000 - HISTORY_TIMESPAN)
  const toTimeSec = Math.floor(req.getEnd && req.getEnd()
    ? parseInt(req.getEnd()) / 1000
    : Date.now() / 1000)
  let typeID = _req.getProfileTypeid && _req.getProfileTypeid()
  if (!typeID) {
    throw new QrynBadRequest('No type provided')
  }
  typeID = parseTypeId(typeID)
  if (!typeID) {
    throw new QrynBadRequest('Invalid type provided')
  }
  const dist = clusterName ? '_dist' : ''
  // const sampleTypeId = typeID.sampleType + ':' + typeID.sampleUnit
  const labelSelector = _req.getLabelSelector && _req.getLabelSelector()

  const typeIdSelector = Sql.Eq(
    'type_id',
    Sql.val(`${typeID.type}:${typeID.periodType}:${typeID.periodUnit}`))
  const serviceNameSelector = serviceNameSelectorQuery(labelSelector)

  const idxReq = (new Sql.Select())
    .select(new Sql.Raw('fingerprint'))
    .from(`${DATABASE_NAME()}.profiles_series_gin`)
    .where(
      Sql.And(
        typeIdSelector,
        serviceNameSelector,
        Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
        Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`)),
        Sql.Eq(
          new Sql.Raw(
            `has(sample_types_units, (${Sql.quoteVal(typeID.sampleType)}, ${Sql.quoteVal(typeID.sampleUnit)}))`
          ),
          1
        )
      )
    )
  labelSelectorQuery(idxReq, labelSelector)
  const withIdxReq = (new Sql.With('idx', idxReq, !!clusterName))
  const mainReq = (new Sql.Select())
    .with(withIdxReq)
    .select([new Sql.Raw('groupArray(payload)'), 'payload'])
    .from([`${DATABASE_NAME()}.profiles${dist}`, 'p'])
    .where(Sql.And(
      new Sql.In('p.fingerprint', 'IN', new Sql.WithReference(withIdxReq)),
      Sql.Gte('p.timestamp_ns', new Sql.Raw(`${fromTimeSec}000000000`)),
      Sql.Lt('p.timestamp_ns', new Sql.Raw(`${toTimeSec}000000000`))))

  const profiles = await clickhouse.rawRequest(mainReq.toString() + ' FORMAT RowBinary',
    null,
    DATABASE_NAME(),
    {
      responseType: 'arraybuffer'
    })
  const binData = Uint8Array.from(profiles.data)

  require('./pprof-bin/pkg/pprof_bin').init_panic_hook()
  const start = process.hrtime.bigint()
  const response = pprofBin.export_trees_pprof(binData)
  logger.debug(`Pprof export took ${process.hrtime.bigint() - start} nanoseconds`)
  return res.code(200).send(Buffer.from(response))
}

const series = async (req, res) => {
  const _req = req.body
  const fromTimeSec = Math.floor(req.getStart && req.getStart()
    ? parseInt(req.getStart()) / 1000
    : Date.now() / 1000 - HISTORY_TIMESPAN)
  const toTimeSec = Math.floor(req.getEnd && req.getEnd()
    ? parseInt(req.getEnd()) / 1000
    : Date.now() / 1000)
  const dist = clusterName ? '_dist' : ''
  const promises = []
  for (const labelSelector of _req.getMatchersList() || []) {
    const specialMatchers = getSpecialMatchers(labelSelector)
    const specialClauses = specialMatchersQuery(specialMatchers.matchers)
    const serviceNameSelector = serviceNameSelectorQuery(labelSelector)
    const idxReq = (new Sql.Select())
      .select(new Sql.Raw('fingerprint'))
      .from(`${DATABASE_NAME()}.profiles_series_gin`)
      .where(
        Sql.And(
          serviceNameSelector,
          Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
          Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`))
        )
      )
    labelSelectorQuery(idxReq, specialMatchers.query)
    const withIdxReq = (new Sql.With('idx', idxReq, !!clusterName))
    const labelsReq = (new Sql.Select())
      .with(withIdxReq)
      .select(
        ['tags', 'tags'],
        ['type_id', 'type_id'],
        ['sample_types_units', '_sample_types_units'])
      .from([`${DATABASE_NAME()}.profiles_series${dist}`, 'p'])
      .join('p.sample_types_units', 'array')
      .where(
        Sql.And(
          serviceNameSelector,
          specialClauses,
          Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
          Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`)),
          new Sql.In('p.fingerprint', 'IN', new Sql.WithReference(withIdxReq))
        )
      )
    console.log(labelsReq.toString())
    promises.push(clickhouse.rawRequest(labelsReq.toString() + ' FORMAT JSON', null, DATABASE_NAME()))
  }
  const resp = await Promise.all(promises)
  const response = new messages.SeriesResponse()
  const labelsSet = []
  resp.forEach(_res => {
    for (const row of _res.data.data) {
      const labels = new types.Labels()
      const _labels = []
      for (const tag of row.tags) {
        const pair = new types.LabelPair()
        pair.setName(tag[0])
        pair.setValue(tag[1])
        _labels.push(pair)
      }
      const typeId = row.type_id.split(':')
      const _pair = (name, val) => {
        const pair = new types.LabelPair()
        pair.setName(name)
        pair.setValue(val)
        return pair
      }
      _labels.push(
        _pair('__name__', typeId[0]),
        _pair('__period_type__', typeId[1]),
        _pair('__period_unit__', typeId[2]),
        _pair('__sample_type__', row._sample_types_units[0]),
        _pair('__sample_unit__', row._sample_types_units[1]),
        _pair('__profile_type__',
          `${typeId[0]}:${row._sample_types_units[0]}:${row._sample_types_units[1]}:${typeId[1]}:${typeId[2]}`)
      )
      labels.setLabelsList(_labels)
      labelsSet.push(labels)
    }
  })
  response.setLabelsSetList(labelsSet)
  return res.code(200).send(Buffer.from(response.serializeBinary()))
}

/**
 *
 * @param query {string}
 */
const getSpecialMatchers = (query) => {
  if (query.length <= 2) {
    return []
  }
  const res = {}
  for (const name of
    ['__name__', '__period_type__', '__period_unit__', '__sample_type__', '__sample_unit__', '__profile_type__']) {
    console.log(`${name}\\s*(=~|!~|=|!=)\\s*("([^"]|\\\\.)+"),*`)
    const re = new RegExp(`${name}\\s*(=~|!~|=|!=)\\s*("([^"]|\\\\.)+"),*`, 'g')
    const pair = re.exec(query)
    if (pair) {
      res[name] = [pair[1], JSON.parse(pair[2])]
    }
    query = query.replaceAll(re, '')
  }
  query = query.replaceAll(/,\s*}$/g, '}')
  return {
    matchers: res,
    query: query
  }
}

const matcherClause = (field, matcher) => {
  let valRul
  const val = matcher[1]
  switch (matcher[0]) {
    case '=':
      valRul = Sql.Eq(new Sql.Raw(field), Sql.val(val))
      break
    case '!=':
      valRul = Sql.Ne(new Sql.Raw(field), Sql.val(val))
      break
    case '=~':
      valRul = Sql.Eq(new Sql.Raw(`match(${(new Sql.Raw(field)).toString()}, ${Sql.quoteVal(val)})`), 1)
      break
    case '!~':
      valRul = Sql.Ne(new Sql.Raw(`match(${(new Sql.Raw(field)).toString()}, ${Sql.quoteVal(val)})`), 1)
  }
  return valRul
}

const specialMatchersQuery = (matchers) => {
  const clauses = []
  if (matchers.__name__) {
    clauses.push(matcherClause("splitByChar(':', type_id)[1]", matchers.__name__))
  }
  if (matchers.__period_type__) {
    clauses.push(matcherClause("splitByChar(':', type_id)[2]", matchers.__period_type__))
  }
  if (matchers.__period_unit__) {
    clauses.push(matcherClause("splitByChar(':', type_id)[3]", matchers.__period_unit__))
  }
  if (matchers.__sample_type__) {
    clauses.push(matcherClause('_sample_types_units.1', matchers.__sample_type__))
  }
  if (matchers.__sample_unit__) {
    clauses.push(matcherClause('_sample_types_units.2', matchers.__sample_unit__))
  }
  if (matchers.__profile_type__) {
    clauses.push(matcherClause(
      "format('{}:{}:{}:{}:{}', (splitByChar(':', type_id) as _parts)[1], _sample_types_units.1, _sample_types_units.2, _parts[2], _parts[3])",
      matchers.__profile_type__))
  }
  if (clauses.length === 0) {
    return Sql.Eq(new Sql.Raw('1'), 1)
  }
  if (clauses.length === 1) {
    return clauses[0]
  }
  return new Sql.And(...clauses)
}

module.exports.init = (fastify) => {
  const fns = {
    profileTypes: profileTypesHandler,
    labelNames: labelNames,
    labelValues: labelValues,
    selectMergeStacktraces: selectMergeStacktraces,
    selectSeries: selectSeries,
    selectMergeProfile: selectMergeProfile,
    series: series
  }
  for (const name of Object.keys(fns)) {
    fastify.post(services.QuerierServiceService[name].path, (req, res) => {
      return fns[name](req, res)
    }, {
      '*': parser(services.QuerierServiceService[name].requestType)
    })
  }
}
