/* Query Handler */
/*
   For doing queries, accepts the following parameters in the query-string:
	query: a logQL query
	limit: max number of entries to return
	start: the start time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
	end: the end time for the query, as a nanosecond Unix epoch (nanoseconds since 1970)
	direction: forward or backward, useful when specifying a limit
	regexp: a regex to filter the returned results, will eventually be rolled into the query language
*/

async function handler (req, res) {
  if (this.debug) console.log('GET /loki/api/v1/query_range')
  if (this.debug) console.log('QUERY: ', req.query)
  // console.log( req.urlData().query.replace('query=',' ') );
  const params = req.query
  const resp = { streams: [] }
  if (!req.query.query) {
    res.send(resp)
    return
  }
  /* remove newlines */
  req.query.query = req.query.query.replace(/\n/g, ' ')
  /* query templates */
  const RATEQUERY = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*)/
  const RATEQUERYWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*) (?:where|WHERE?) (.*)/
  const RATEQUERYNOWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.([\S]+)\s?$/
  const RATEQUERYMETRICS = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\)/

  if (!req.query.query) {
    res.code(400).send('invalid query')
  } else if (RATEQUERYWHERE.test(req.query.query)) {
    var s = RATEQUERYWHERE.exec(req.query.query)
    console.log('tags', s)
    var JSON_labels = {
      db: s[5],
      table: s[6],
      interval: s[4] || 60,
      tag: s[2],
      metric: s[1] + '(' + s[3] + ')',
      where: s[7]
    }
    this.scanClickhouse(JSON_labels, res, params)
  } else if (RATEQUERYNOWHERE.test(req.query.query)) {
    var s = RATEQUERYNOWHERE.exec(req.query.query)
    console.log('tags', s)
    var JSON_labels = {
      db: s[5],
      table: s[6],
      interval: s[4] || 60,
      tag: s[2],
      metric: s[1] + '(' + s[3] + ')'
    }
    this.scanClickhouse(JSON_labels, res, params)
  } else if (RATEQUERY.test(req.query.query)) {
    var s = RATEQUERY.exec(req.query.query)
    console.log('tags', s)
    var JSON_labels = {
      db: s[5],
      table: s[6],
      interval: s[4] || 60,
      tag: s[2],
      metric: s[1] + '(' + s[3] + ')'
    }
    this.scanClickhouse(JSON_labels, res, params)
  } else if (req.query.query.startsWith('clickhouse(')) {
    try {
      const query = /\{(.*?)\}/g.exec(req.query.query)[1] || req.query.query
      var queries = query.replace(/\!?="/g, ':"')
      var JSON_labels = this.toJSON(queries)
    } catch (e) {
      console.error(e, queries)
      res.send(resp)
    }
    if (this.debug) console.log('SCAN CLICKHOUSE', JSON_labels, params)
    this.scanClickhouse(JSON_labels, res, params)
  } else {
    try {
      await this.scanFingerprints(
        req.query,
        { res: res.raw }
      )
    } catch (e) {
      console.log(e)
      res.send(resp)
    }
    if (this.debug) console.log('SCAN LABELS', JSON_labels, params)
  }
};

module.exports = handler
