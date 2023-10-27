function handler (req, res) {
  req.log.debug('unsupported', req.url)
  return res.code(404).send('404 Not Supported')
}

module.exports = handler
