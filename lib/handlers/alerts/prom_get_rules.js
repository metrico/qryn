const { getAll } = require('../../db/alerting')
const format = require('date-fns/formatRFC3339')
const { durationToMs } = require('../../../common')
const { assertEnabled } = require('./common')

module.exports = (req, res) => {
  assertEnabled()
  const rules = getAll()
  const groups = []
  for (const ns of Object.entries(rules)) {
    for (const group of Object.entries(ns[1])) {
      groups.push({
        name: group[0],
        file: ns[0],
        rules: Object.values(group[1].rules).map(rul2Res)
      })
    }
  }
  res.send({
    status: 'success',
    data: {
      groups: groups
    }
  })
}

/**
 *
 * @param rul {alerting.rule}
 */
const rul2Res = (rul) => {
  const alerts = rul && rul._watcher && rul._watcher.getLastAlert()
    ? [{
        ...rul._watcher.getLastAlert(),
        activeAt: format(rul._watcher.getLastAlert().activeAt)
      }]
    : []
  const health = rul && rul._watcher ? rul._watcher.health : 'nodata'
  const lastError = rul && rul._watcher ? rul._watcher.lastError : ''
  const state = rul && rul._watcher ? rul._watcher.state || 'normal' : 'normal'
  return {
    name: rul.alert,
    query: rul.expr,
    duration: durationToMs(rul.for || '30s') / 1000,
    alerts: alerts,
    labels: rul.labels || {},
    annotations: rul.annotations || {},
    health: health,
    lastError: lastError,
    state: state,
    type: 'alerting',
    lastEvaluation: format(new Date()),
    evaluationTime: 0.01
  }
}
