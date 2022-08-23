function handler (req, res) {
  req.log.debug('unsupported', req.url)
  res.send('404 Not Supported')
}

module.exports = handler
