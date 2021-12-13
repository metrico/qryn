const { getAll } = require('../../db/alerting')
const format = require('date-fns/formatRFC3339')
const { durationToMs } = require('../../../common')
module.exports = (req, res) => {
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
  return {
    state: 'inactive',
    name: rul.alert,
    query: rul.expr,
    duration: durationToMs(rul.for) / 1000,
    alerts: [],
    labels: rul.labels,
    health: 'nodata',
    lastError: '',
    type: 'alerting',
    lastEvaluation: format(new Date()),
    evaluationTime: 0.01
  }
}
