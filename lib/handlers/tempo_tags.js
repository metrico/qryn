const { asyncLogError } = require('../../common')

async function handler (req, res) {
  try {
    const resp = await this.queryTempoTags()
    return res.send(resp.map(e => e.key))
  } catch (e) {
    asyncLogError(e, req.log)
    res.code(500)
  }
}

module.exports = handler
