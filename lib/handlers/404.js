function handler (req, res) {
  if (this.debug) console.log('unsupported', req.params, req.query, req.body)
  res.send('404 Not Supported')
}

module.exports = handler
