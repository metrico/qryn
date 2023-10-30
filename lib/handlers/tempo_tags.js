const { asyncLogError } = require('../../common')
const { queryTempoTags } = require('../db/clickhouse')
async function handler (req, res) {
  try {
    const resp = await queryTempoTags()
    return res.send(resp.map(e => e.key))
  } catch (e) {
    asyncLogError(e, req.log)
    res.code(500)
  }
}

module.exports = handler
