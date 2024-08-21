const { QrynBadRequest } = require('../lib/handlers/errors')
const { parseTypeId, serviceNameSelectorQuery, labelSelectorQuery } = require('./shared')
const { clusterName } = require('../common')
const Sql = require('@cloki/clickhouse-sql')
const { DATABASE_NAME } = require('../lib/utils')
const types = require('./types/v1/types_pb')
const clickhouse = require('../lib/db/clickhouse')
const messages = require('./querier_pb')

const selectSeriesImpl = async (fromTimeSec, toTimeSec, payload) => {
  const _req = payload
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
  ).groupBy('timestamp_ms', 'fingerprint')
    .orderBy(['fingerprint', 'ASC'], ['timestamp_ms', 'ASC'])
  const strMainReq = mainReq.toString()
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
  return resp
}

module.exports = {
  selectSeriesImpl
}
