
async function handler (req, res) {
  try {
    const resp = await this.queryTempoTags()
    res.send(resp.map(e => e.key))
  } catch (e) {
    req.log.error(e)
    res.code(500)
  }
}

module.exports = handler
