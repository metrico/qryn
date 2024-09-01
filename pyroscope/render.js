const { parseQuery } = require('./shared')
const { mergeStackTraces } = require('./merge_stack_traces')
const querierMessages = require('./querier_pb')
const { selectSeriesImpl } = require('./select_series')

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
  let pTimeline = null
  for (const series of selectSeries.getSeriesList()) {
    if (!pTimeline) {
      pTimeline = timeline(series,
        fromTimeSec * 1000,
        toTimeSec * 1000,
        timelineStep)
      continue
    }
    const _timeline = timeline(series,
      fromTimeSec * 1000,
      toTimeSec * 1000,
      timelineStep)
    pTimeline.samples = pTimeline.samples.map((v, i) => v + _timeline.samples[i])
  }
  const fb = toFlamebearer(mergeStackTrace.getFlamegraph(), parsedQuery.profileType)
  fb.flamebearerProfileV1.timeline = pTimeline

  if (groupBy.length > 0) {
    const pGroupedTimelines = {}
    fb.flamebearerProfileV1.groups = {}
    for (const series of selectSeries.getSeriesList()) {
      const _key = {}
      for (const label of series.getLabelsList()) {
        if (groupBy.includes(label.getName())) {
          _key[label.getName()] = label.getValue()
        }
      }
      const key = '{' + Object.entries(_key).map(e => `${e[0]}=${JSON.stringify(e[1])}`)
        .sort().join(', ') + '}'
      if (!pGroupedTimelines[key]) {
        pGroupedTimelines[key] = timeline(series,
          fromTimeSec * 1000,
          toTimeSec * 1000,
          timelineStep)
      } else {
        const _timeline = timeline(series,
          fromTimeSec * 1000,
          toTimeSec * 1000,
          timelineStep)
        pGroupedTimelines[key].samples = pGroupedTimelines[key].samples.map((v, i) => v + _timeline.samples[i])
      }
    }
    fb.flamebearerProfileV1.groups = pGroupedTimelines
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


const init = (fastify) => {
  fastify.get('/pyroscope/render', render)
}

module.exports = {
  init,
  parseQuery,
  toFlamebearer
}
