// Query Handler
function handler (req, res) {
  if (this.debug) console.log('GET /loki/api/v1/query')
  if (this.debug) console.log('QUERY: ', req.query)
  const query = req.query.query.replace(/\!?="/g, ':"')

  // console.log( req.urlData().query.replace('query=',' ') );
  const all_values = this.labels.get(query.name)
  if (!all_values || all_values.length == 0) {
    var resp = {
      status: 'success',
      data: { resultType: 'streams', result: [] }
    }
  } else {
    var resp = { values: all_values }
  }
  if (this.debug) console.log('LABEL', query.name, 'VALUES', all_values)
  res.send(resp)
};

module.exports = handler
