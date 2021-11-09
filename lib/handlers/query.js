// Query Handler
function handler (req, res) {
  if (this.debug) console.log('GET /loki/api/v1/query')
  if (this.debug) console.log('QUERY: ', req.query)
  const query = req.query.query.replace(/\!?="/g, ':"')

  // console.log( req.urlData().query.replace('query=',' ') );
  const allValues = this.labels.get(query.name)
  if (!allValues || allValues.length == 0) {
    var resp = {
      status: 'success',
      data: { resultType: 'streams', result: [] }
    }
  } else {
    var resp = { values: allValues }
  }
  if (this.debug) console.log('LABEL', query.name, 'VALUES', allValues)
  res.send(resp)
};

module.exports = handler
