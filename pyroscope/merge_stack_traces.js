const { checkVersion, DATABASE_NAME } = require('../lib/utils')
const Sql = require('@cloki/clickhouse-sql')
const { clusterName } = require('../common')
const clickhouse = require('../lib/db/clickhouse')
const { readULeb32 } = require('./pprof')
const pprofBin = require('./pprof-bin/pkg')
const {
  serviceNameSelectorQuery,
  labelSelectorQuery
} = require('./shared')

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

let ctxIdx = 0

const mergeStackTraces = async (typeRegex, sel, fromTimeSec, toTimeSec, log) => {
  const dist = clusterName ? '_dist' : ''
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
  const profiles = await clickhouse.rawRequest(sqlReq.toString() + ' FORMAT RowBinary',
    null,
    DATABASE_NAME(),
    {
      responseType: 'arraybuffer'
    })
  const binData = Uint8Array.from(profiles.data)
  log.debug(`selectMergeStacktraces: profiles downloaded: ${binData.length / 1025}kB in ${Date.now() - start}ms`)
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
    log.debug(`merge_pprof: ${mergePprofLat / BigInt(1000000)}ms`)
    log.debug(`merge_tree: ${mergeTreeLat / BigInt(1000000)}ms`)
    log.debug(`export_tree: ${exportTreeLat / BigInt(1000000)}ms`)
    return Buffer.from(resp)
  } finally {
    try { pprofBin.drop_tree(_ctxIdx) } catch (e) {}
  }
}

module.exports = {
  mergeStackTraces
}