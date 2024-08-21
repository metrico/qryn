const { parseQuery, toFlamebearer } = require('./render')
const { importStackTraces, newCtxIdx } = require('./merge_stack_traces')
const pprofBin = require('./pprof-bin/pkg')
const querierMessages = require('./querier_pb')
const types = require('./types/v1/types_pb')

const renderDiff = async (req, res) => {
  const [leftQuery, leftFromTimeSec, leftToTimeSec] =
    parseParams(req.query.leftQuery, req.query.leftFrom, req.query.leftUntil);
  const [rightQuery, rightFromTimeSec, rightToTimeSec] =
    parseParams(req.query.rightQuery, req.query.rightFrom, req.query.rightUntil);
  if (leftQuery.typeId != rightQuery.typeId) {
    res.code(400).send('Different type IDs')
    return
  }
  const leftCtxIdx = newCtxIdx()
  await importStackTraces(leftQuery.typeDesc, '{' + leftQuery.labelSelector + '}', leftFromTimeSec, leftToTimeSec, req.log, leftCtxIdx, true)
  const rightCtxIdx = newCtxIdx()
  await importStackTraces(rightQuery.typeDesc, '{' + rightQuery.labelSelector + '}', rightFromTimeSec, rightToTimeSec, req.log, rightCtxIdx, true)
  const flamegraphDiffBin = pprofBin.diff_tree(leftCtxIdx, rightCtxIdx,
    `${leftQuery.typeDesc.sampleType}:${leftQuery.typeDesc.sampleUnit}`)
  const profileType = new types.ProfileType()
  profileType.setId(leftQuery.typeId)
  profileType.setName(leftQuery.typeDesc.type)
  profileType.setSampleType(leftQuery.typeDesc.sampleType)
  profileType.setSampleUnit(leftQuery.typeDesc.sampleUnit)
  profileType.setPeriodType(leftQuery.typeDesc.periodType)
  profileType.setPeriodUnit(leftQuery.typeDesc.periodUnit)
  const diff = querierMessages.FlameGraphDiff.deserializeBinary(flamegraphDiffBin)
  return res.code(200).send(diffToFlamegraph(diff, profileType).flamebearerProfileV1)
}

/**
 *
 * @param diff
 * @param type
 */
const diffToFlamegraph = (diff, type) => {
  const fg = new querierMessages.FlameGraph()
  fg.setNamesList(diff.getNamesList())
  fg.setLevelsList(diff.getLevelsList())
  fg.setTotal(diff.getTotal())
  fg.setMaxSelf(diff.getMaxSelf())
  const fb = toFlamebearer(fg, type)
  fb.flamebearerProfileV1.leftTicks = diff.getLeftticks()
  fb.flamebearerProfileV1.rightTicks = diff.getRightticks()
  fb.flamebearerProfileV1.metadata = {
    ...(fb.flamebearerProfileV1.metadata || {}),
    format: 'double'
  }
  return fb
}

const parseParams = (query, from, until) => {
  const parsedQuery = parseQuery(query)
  const fromTimeSec = from
    ? Math.floor(parseInt(from) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  const toTimeSec = until
    ? Math.floor(parseInt(until) / 1000)
    : Math.floor((Date.now() - 1000 * 60 * 60 * 48) / 1000)
  if (!parsedQuery) {
    throw new Error('Invalid query')
  }
  return [parsedQuery, fromTimeSec, toTimeSec]
}

const init = (fastify) => {
  fastify.get('/pyroscope/render-diff', renderDiff)
}

module.exports = {
  renderDiff,
  init
}
