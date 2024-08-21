const { parseTypeId } = require('./shared')
const { mergeStackTraces } = require('./merge_stack_traces')
const querierMessages = require('./querier_pb')
const { selectSeriesImpl } = require('./select_series')
const types = require('./types/v1/types_pb')

const render = async (req, res) => {
  const query = req.query.query
  const parsedQuery = parseQuery(query)
  const fromTimeSec = req.query.from
    ? Math.floor(parseInt(req.query.from) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  const toTimeSec = req.query.until
    ? Math.floor(parseInt(req.query.until) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  if (!parsedQuery) {
    return res.sendStatus(400).send('Invalid query')
  }
  const groupBy = req.query.groupBy || []
  let agg = ''
  switch (req.query.aggregation) {
    case 'sum':
      agg = 'sum'
      break
    case 'avg':
      agg = 'avg'
      break
  }
  if (req.query.format === 'dot') {
    return res.sendStatus(400).send('Dot format is not supported')
  }
  const promises = []
  promises.push(mergeStackTraces(
    parsedQuery.typeDesc,
    '{' + parsedQuery.labelSelector + '}',
    fromTimeSec,
    toTimeSec,
    req.log))

  const timelineStep = calcIntervalSec(fromTimeSec, toTimeSec)
  promises.push(selectSeriesImpl(
    fromTimeSec,
    toTimeSec,
    {
      getProfileTypeid: () => parsedQuery.typeId,
      getLabelSelector: () => `{${parsedQuery.labelSelector}}`,
      getGroupByList: () => groupBy,
      getStep: () => timelineStep,
      getAggregation: () => agg
    }
  ))
  const [bMergeStackTrace, selectSeries] =
    await Promise.all(promises)
  const mergeStackTrace = querierMessages.SelectMergeStacktracesResponse.deserializeBinary(bMergeStackTrace)
  let series = new types.Series()
  if (selectSeries.getSeriesList().length === 1) {
    series = selectSeries.getSeriesList()[0]
  }
  const fb = toFlamebearer(mergeStackTrace.getFlamegraph(), parsedQuery.profileType)
  fb.flamebearerProfileV1.timeline = timeline(series,
    fromTimeSec * 1000,
    toTimeSec * 1000,
    timelineStep)

  if (groupBy.length > 0) {
    fb.flamebearerProfileV1.groups = {}
    let key = '*'
    series.getSeriesList().forEach((_series) => {
      _series.getLabelsList().forEach((label) => {
        key = label.getName() === groupBy[0] ? label.getValue() : key
      })
    })
    fb.flamebearerProfileV1.groups[key] = timeline(series,
      fromTimeSec * 1000,
      toTimeSec * 1000,
      timelineStep)
  }
  res.code(200)
  res.headers({ 'Content-Type': 'application/json' })
  return res.send(Buffer.from(JSON.stringify(fb.flamebearerProfileV1)))
}

/**
 *
 * @param fg
 * @param profileType
 * @returns {Flamebearer}
 */
function toFlamebearer (fg, profileType) {
  if (!fg) {
    fg = new querierMessages.FlameGraph()
  }
  let unit = profileType.getSampleUnit()
  let sampleRate = 100
  switch (profileType.getSampleType()) {
    case 'inuse_objects':
    case 'alloc_objects':
    case 'goroutine':
    case 'samples':
      unit = 'objects'
      break
    case 'cpu':
      unit = 'samples'
      sampleRate = 1000000000
  }
  /** @type {flamebearerV1} */
  const flameBearer = {
    levels: fg.getLevelsList().map(l => l.getValuesList().map(v => v)),
    maxSelf: fg.getMaxSelf(),
    names: fg.getNamesList(),
    numTicks: fg.getTotal()
  }
  /** @type {flamebearerMetadataV1} */
  const metadata = {
    format: 'single',
    units: unit,
    name: profileType.getSampleType(),
    sampleRate: sampleRate
  }

  return {
    version: 1,
    flamebearerProfileV1: {
      metadata: metadata,
      flamebearer: flameBearer
    }
  }
}

/**
 *
 * @param fromSec {number}
 * @param toSec {number}
 * @returns {number}
 */
function calcIntervalSec (fromSec, toSec) {
  return Math.max(Math.ceil((toSec - fromSec) / 1500), 15)
}

/**
 *
 * @param series
 * @param startMs
 * @param endMs
 * @param durationDeltaSec
 * @returns {flamebearerTimelineV1}
 */
function timeline (series, startMs, endMs, durationDeltaSec) {
  const durationDeltaMs = durationDeltaSec * 1000
  startMs = Math.floor(startMs / durationDeltaMs) * durationDeltaMs
  endMs = Math.floor(endMs / durationDeltaMs) * durationDeltaMs
  const startS = Math.floor(startMs / 1000)
  /** @type {flamebearerTimelineV1} */
  const timeline = {
    durationDelta: durationDeltaSec,
    startTime: startS,
    samples: []
  }
  if (startMs >= endMs) {
    return timeline
  }
  const points = boundPointsToWindow(series.getPointsList(), startMs, endMs)
  if (points.length < 1) {
    const n = sizeToBackfill(startMs, endMs, durationDeltaSec)
    if (n > 0) {
      timeline.samples = new Array(n).fill(0)
    }
    return timeline
  }

  let n = sizeToBackfill(startMs, parseInt(points[0].getTimestamp()), durationDeltaSec)
  const samples = n > 0 ? Array(n).fill(0) : []
  let prev = points[0]
  for (const p of points) {
    n = sizeToBackfill(parseInt(prev.getTimestamp()), parseInt(p.getTimestamp()), durationDeltaSec)
    Array.prototype.push.apply(samples, new Array(Math.max(0, n - 1)).fill(0))
    samples.push(p.getValue())
    prev = p
  }
  Array.prototype.push.apply(samples,
    new Array(Math.max(0, sizeToBackfill(startMs, endMs, durationDeltaSec) - samples.length))
      .fill(0)
  )
  timeline.samples = samples
  return timeline
}

/**
 *
 * @param points {[]}
 * @param startMs {number}
 * @param endMs {number}
 */
function boundPointsToWindow (points, startMs, endMs) {
  const startIdx = points.findIndex((v) => v.getTimestamp() >= startMs)
  const endIdx = points.findLastIndex((v) => v.getTimestamp() < endMs)
  return points.slice(startIdx, endIdx + 1)
}

/**
 *
 * @param startMs {number}
 * @param endMs {number}
 * @param stepSec {number}
 * @returns {number}
 */
function sizeToBackfill (startMs, endMs, stepSec) {
  return Math.floor((endMs - startMs) / (stepSec * 1000))
}

/**
 *
 * @param query {string}
 */
const parseQuery = (query) => {
  query = query.trim()
  const match = query.match(/^([^{\s]+)\s*(\{(.*)})?$/)
  if (!match) {
    return null
  }
  const typeId = match[1]
  const typeDesc = parseTypeId(typeId)
  let strLabels = (match[3] || '').trim()
  const labels = []
  while (strLabels && strLabels !== '' && strLabels !== '}') {
    const m = strLabels.match(/^([,{])\s*([A-Za-z0-9_]+)\s*(!=|!~|=~|=)\s*("([^"\\]|\\.)*")/)
    if (!m) {
      throw new Error('Invalid label selector')
    }
    labels.push([m[2], m[3], m[4]])
    strLabels = strLabels.substring(m[0].length).trim()
  }
  const profileType = new types.ProfileType()
  profileType.setId(typeId)
  profileType.setName(typeDesc.type)
  profileType.setSampleType(typeDesc.sampleType)
  profileType.setSampleUnit(typeDesc.sampleUnit)
  profileType.setPeriodType(typeDesc.periodType)
  profileType.setPeriodUnit(typeDesc.periodUnit)
  return {
    typeId,
    typeDesc,
    labels,
    labelSelector: strLabels,
    profileType
  }
}

const init = (fastify) => {
  fastify.get('/pyroscope/render', render)
}

module.exports = {
  init,
  parseQuery,
  toFlamebearer
}
