const { CLokiBadRequest } = require('../errors')
const clickhouse = require('../../db/clickhouse')
const yaml = require('yaml')

module.exports = async (req, res) => {
  const result = {
    fake: [
      {
        name: 'fake',
        interval: '1m',
        rules: [{
          alert: 'fake',
          expr: 'rate({test_id="_TEST_"}[1m])',
          for: '1m',
          annotations:
            {
              a1: 'fake'
            },
          labels: { l1: 'fake' }
        }]
      }
    ]
  }
  //const result = await clickhouse.getAlertRules(limit, offset)
  //const count = await clickhouse.getAlertRulesCount()
  console.log(yaml.stringify(result))
  res.header('Content-Type', 'yaml').send(yaml.stringify(result))
}
