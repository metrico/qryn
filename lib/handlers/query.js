// Query Handler
function handler (req, res) {
  if (this.debug) console.log('GET /loki/api/v1/query')
  if (this.debug) console.log('QUERY: ', req.query)
  const query = req.query.query.replace(/\!?="/g, ':"')

  // console.log( req.urlData().query.replace('query=',' ') );
  const allValues = this.labels.get(query.name)
  if (this.debug) console.log('LABEL', query.name, 'VALUES', allValues)
  if (!allValues || allValues.length === 0) {
    res.send({
      status: 'success',
      data: { resultType: 'streams', result: [] }
    })
  } else {
    res.send({ values: allValues })
  }
}

module.exports = handler
