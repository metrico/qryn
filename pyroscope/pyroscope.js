const messages = require('./querier_pb')
const types = require('./types/v1/types_pb')
const services = require('./querier_grpc_pb')
const clickhouse = require('../lib/db/clickhouse')
const { DATABASE_NAME } = require('../lib/utils')
const Sql = require('@cloki/clickhouse-sql')
const pprofBin = require('./pprof-bin/pkg/pprof_bin')
const { QrynBadRequest } = require('../lib/handlers/errors')
const { clusterName } = require('../common')
const logger = require('../lib/logger')
const jsonParsers = require('./json_parsers')
const renderDiff = require('./render_diff')
const {
  parser,
  wrapResponse,
  parseTypeId,
  serviceNameSelectorQuery,
  labelSelectorQuery,
  HISTORY_TIMESPAN
} = require('./shared')
const settings = require('./settings')
const { mergeStackTraces, newCtxIdx } = require('./merge_stack_traces')
const { selectSeriesImpl } = require('./select_series')
const render = require('./render')

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
  return _res
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
  return resp
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
  return resp
}

const selectMergeStacktraces = async (req, res) => {
  return await selectMergeStacktracesV2(req, res)
}

const selectMergeStacktracesV2 = async (req, res) => {
  const typeRegex = parseTypeId(req.body.getProfileTypeid())
  const sel = req.body.getLabelSelector()
  const fromTimeSec = req.body && req.body.getStart()
    ? Math.floor(parseInt(req.body.getStart()) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  const toTimeSec = req.body && req.body.getEnd()
    ? Math.floor(parseInt(req.body.getEnd()) / 1000)
    : Math.floor(Date.now() / 1000)
  const resBuffer = await mergeStackTraces(typeRegex, sel, fromTimeSec, toTimeSec, req.log)
  return res.code(200).send(resBuffer)
}

const selectSeries = async (req, res) => {
  const fromTimeSec = Math.floor(req.getStart && req.getStart()
    ? parseInt(req.getStart()) / 1000
    : Date.now() / 1000 - HISTORY_TIMESPAN)
  const toTimeSec = Math.floor(req.getEnd && req.getEnd()
    ? parseInt(req.getEnd()) / 1000
    : Date.now() / 1000)
  return selectSeriesImpl(fromTimeSec, toTimeSec, req.body)
}

const selectMergeProfile = async (req, res) => {
  const ctx = newCtxIdx()
  try {
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
      .select([new Sql.Raw('payload'), 'payload'])
      .from([`${DATABASE_NAME()}.profiles${dist}`, 'p'])
      .where(Sql.And(
        new Sql.In('p.fingerprint', 'IN', new Sql.WithReference(withIdxReq)),
        Sql.Gte('p.timestamp_ns', new Sql.Raw(`${fromTimeSec}000000000`)),
        Sql.Lt('p.timestamp_ns', new Sql.Raw(`${toTimeSec}000000000`))))
      .orderBy(new Sql.Raw('timestamp_ns'))
    const approxReq = (new Sql.Select())
      .select(
        [new Sql.Raw('sum(length(payload))'), 'size'],
        [new Sql.Raw('count()'), 'count']
      )
      .from([new Sql.Raw('(' + mainReq.toString() + ')'), 'main'])
    const approx = await clickhouse.rawRequest(
      approxReq.toString() + ' FORMAT JSON', null, DATABASE_NAME()
    )
    const approxData = approx.data.data[0]
    logger.debug(`Approximate size: ${approxData.size} bytes, profiles count: ${approxData.count}`)
    const chunksCount = Math.max(Math.ceil(approxData.size / (50 * 1024 * 1024)), 1)
    logger.debug(`Request is processed in: ${chunksCount} chunks`)
    const chunkSize = Math.ceil(approxData.count / chunksCount)
    const promises = []
    require('./pprof-bin/pkg/pprof_bin').init_panic_hook()
    let processNs = BigInt(0)
    const start = process.hrtime.bigint()

    for (let i = 0; i < chunksCount; i++) {
      promises.push((async (i) => {
        logger.debug(`Processing chunk ${i}`)
        const profiles = await clickhouse.rawRequest(mainReq.toString() + ` LIMIT ${chunkSize} OFFSET ${i * chunkSize} FORMAT RowBinary`,
          null,
          DATABASE_NAME(),
          {
            responseType: 'arraybuffer'
          })
        const binData = Uint8Array.from(profiles.data)
        logger.debug(`Chunk ${i} - ${binData.length} bytes`)
        const start = process.hrtime.bigint()
        pprofBin.merge_trees_pprof(ctx, binData)
        const end = process.hrtime.bigint()
        processNs += end - start
      })(i))
    }
    await Promise.all(promises)
    const response = pprofBin.export_trees_pprof(ctx)
    const end = process.hrtime.bigint()

    logger.debug(`Pprof merge took ${processNs} nanoseconds`)
    logger.debug(`Pprof load + merge took ${end - start} nanoseconds`)
    return res.code(200).send(Buffer.from(response))
  } finally {
    pprofBin.drop_tree(ctx)
  }
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
    promises.push(clickhouse.rawRequest(labelsReq.toString() + ' FORMAT JSON', null, DATABASE_NAME()))
  }
  if ((_req.getMatchersList() || []).length === 0) {
    const labelsReq = (new Sql.Select())
      .select(
        ['tags', 'tags'],
        ['type_id', 'type_id'],
        ['sample_types_units', '_sample_types_units'])
      .from([`${DATABASE_NAME()}.profiles_series${dist}`, 'p'])
      .join('p.sample_types_units', 'array')
      .where(
        Sql.And(
          Sql.Gte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(fromTimeSec)}))`)),
          Sql.Lte('date', new Sql.Raw(`toDate(FROM_UNIXTIME(${Math.floor(toTimeSec)}))`))
        )
      )
    promises.push(clickhouse.rawRequest(labelsReq.toString() + ' FORMAT JSON', null, DATABASE_NAME()))
  }
  const resp = await Promise.all(promises)
  const response = new messages.SeriesResponse()
  const labelsSet = []
  const filterLabelNames = _req.getLabelNamesList() || null
  resp.forEach(_res => {
    for (const row of _res.data.data) {
      const labels = new types.Labels()
      let _labels = []
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
      if (filterLabelNames && filterLabelNames.length) {
        _labels = _labels.filter((l) => filterLabelNames.includes(l.getName()))
      }
      if (_labels.length > 0) {
        labels.setLabelsList(_labels)
        labelsSet.push(labels)
      }
    }
  })
  response.setLabelsSetList(labelsSet)
  return response
}

/**
 *
 * @param query {string}
 */
const getSpecialMatchers = (query) => {
  if (query.length <= 2) {
    return {
      matchers: {},
      query: query
    }
  }
  const res = {}
  for (const name of
    ['__name__', '__period_type__', '__period_unit__', '__sample_type__', '__sample_unit__', '__profile_type__']) {
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

const getProfileStats = async (req, res) => {
  const sql = `
with non_empty as (select any(1) as non_empty from profiles limit 1),
     min_date as (select min(date) as min_date, max(date) as max_date from profiles_series),
     min_time as (
        select intDiv(min(timestamp_ns), 1000000) as min_time,
               intDiv(max(timestamp_ns), 1000000) as max_time
        from profiles
        where timestamp_ns < toUnixTimestamp((select any (min_date) from min_date) + INTERVAL '1 day') * 1000000000 OR
            timestamp_ns >= toUnixTimestamp((select any(max_date) from min_date)) * 1000000000
    )
select
    (select any(non_empty) from non_empty) as non_empty,
    (select any(min_time) from min_time) as min_time,
    (select any(max_time) from min_time) as max_time
`
  const sqlRes = await clickhouse.rawRequest(sql + ' FORMAT JSON', null, DATABASE_NAME())
  const response = new types.GetProfileStatsResponse()
  response.setDataIngested(!!sqlRes.data.data[0].non_empty)
  response.setOldestProfileTime(sqlRes.data.data[0].min_time)
  response.setNewestProfileTime(sqlRes.data.data[0].max_time)
  return response
}

const analyzeQuery = async (req, res) => {
  const query = req.body.getQuery()
  const fromTimeSec = Math.floor(req.getStart && req.getStart()
    ? parseInt(req.getStart()) / 1000
    : Date.now() / 1000 - HISTORY_TIMESPAN)
  const toTimeSec = Math.floor(req.getEnd && req.getEnd()
    ? parseInt(req.getEnd()) / 1000
    : Date.now() / 1000)

  const scope = new messages.QueryScope()
  scope.setComponentType('store')
  scope.setComponentCount(1)
  const impact = new messages.QueryImpact()
  impact.setTotalBytesInTimeRange(10 * 1024 * 1024)
  impact.setTotalQueriedSeries(15)
  impact.setDeduplicationNeeded(false)
  const response = new messages.AnalyzeQueryResponse()
  response.setQueryScopesList([scope])
  response.setQueryImpact(impact)
  return response
}

module.exports.init = (fastify) => {
  const fns = {
    profileTypes: profileTypesHandler,
    labelNames: labelNames,
    labelValues: labelValues,
    selectMergeStacktraces: selectMergeStacktraces,
    selectSeries: selectSeries,
    selectMergeProfile: selectMergeProfile,
    series: series,
    getProfileStats: getProfileStats,
    analyzeQuery: analyzeQuery
  }
  const parsers = {
    series: jsonParsers.series,
    getProfileStats: jsonParsers.getProfileStats,
    labelNames: jsonParsers.labelNames,
    analyzeQuery: jsonParsers.analyzeQuery
  }
  for (const name of Object.keys(fns)) {
    fastify.post(services.QuerierServiceService[name].path, (req, res) => {
      return wrapResponse(fns[name])(req, res)
    }, {
      'application/json': parsers[name],
      '*': parser(services.QuerierServiceService[name].requestType)
    })
  }
  settings.init(fastify)
  render.init(fastify)
  renderDiff.init(fastify)
}
