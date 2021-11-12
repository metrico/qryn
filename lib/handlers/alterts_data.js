const axios = require('axios')

async function handler (req, res) {
  req.body = JSON.parse(req.body)
  if (!process.env.ALERTMAN_URL) {
    res.send('ok')
    return
  }
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
  res.send('ok')
}

module.exports = handler
