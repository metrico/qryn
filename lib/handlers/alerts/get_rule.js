const { CLokiNotFound } = require('../errors')
const clickhouse = require('../../db/clickhouse')
const yaml = require('yaml')

module.exports = async (req, res) => {
  /*const name = req.params.name
  const rule = await clickhouse.getAlertRule(name)
  if (!rule) {
    throw new CLokiNotFound(`Rule with name '${name}' not found`)
  }
  res.send(rule)*/
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
  console.log(yaml.stringify(result.fake[0]))
  res.header('Content-Type', 'yaml').send(yaml.stringify(result.fake[0]))
}
