const axios = require('axios')
const logger = require('../logger')

async function handler (req, res) {
  req.body = JSON.parse(req.body)
  if (!process.env.ALERTMAN_URL) {
    res.send('ok')
    return
  }
  try {
    logger.info(`POSTING \`${process.env.ALERTMAN_URL}/api/v2/alerts\` ${req.body.data.length}`)
    await axios.post(`${process.env.ALERTMAN_URL}/api/v2/alerts`,
      req.body.data.map(d => ({
        labels: {
          ...JSON.parse(d.labels),
          alertname: 'n1'
        },
        annotations: {
          string: d.string
        },
        generatorURL: 'http://cLoki/alerts'
      }))
    )
  } catch (err) {
    logger.error({ err }, 'SEND ERROR')
    throw err
  }
  res.send('ok')
}

module.exports = handler
