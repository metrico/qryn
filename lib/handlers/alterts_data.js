const axios = require('axios')

async function handler (req, res) {
  req.body = JSON.parse(req.body)
  if (!process.env.ALERTMAN_URL) {
    res.send('ok')
    return
  }
  try {
    console.log(`POSTING \`${process.env.ALERTMAN_URL}/api/v2/alerts\` ${req.body.data.length}`)
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
  } catch (e) {
    console.log('SEND ERROR')
    console.log(e.message)
    throw e
  }
  res.send('ok')
}

module.exports = handler
