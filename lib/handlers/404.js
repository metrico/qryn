function handler (req, res) {
  req.log.debug('unsupported')
  res.send('404 Not Supported')
}

module.exports = handler
