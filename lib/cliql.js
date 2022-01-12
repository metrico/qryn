const { toJSON } = require('./utils')
const RATEQUERY = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*)/
const RATEQUERYWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.(.*) (?:where|WHERE?) (.*)/
const RATEQUERYNOWHERE = /(.*) by \((.*)\) \(rate\((.*)\[(.*)\]\)\) from (.*)\.([\S]+)\s?$/

/**
 *
 * @param query {string}
 * @returns {{
 * metric: string,
 * interval: (string|number),
 * tag: string,
 * db: string,
 * table: string
 * } | undefined}
 */
module.exports.parseCliQL = (query) => {
  if (RATEQUERYWHERE.test(query)) {
    const s = RATEQUERYWHERE.exec(query)
    return {
      db: s[5],
      table: s[6],
      interval: s[4] || 60,
      tag: s[2],
      metric: s[1] + '(' + s[3] + ')',
      where: s[7]
    }
  } else if (RATEQUERYNOWHERE.test(query)) {
    const s = RATEQUERYNOWHERE.exec(query)
    return {
      db: s[5],
      table: s[6],
      interval: s[4] || 60,
      tag: s[2],
      metric: s[1] + '(' + s[3] + ')'
    }
  } else if (RATEQUERY.test(query)) {
    const s = RATEQUERY.exec(query)
    return {
      db: s[5],
      table: s[6],
      interval: s[4] || 60,
      tag: s[2],
      metric: s[1] + '(' + s[3] + ')'
    }
  } else if (query.startsWith('clickhouse(')) {
    let queries = null
    const _query = /\{(.*?)\}/g.exec(query)[1] || query
    queries = _query.replace(/\!?="/g, ':"')
    return toJSON(queries)
  }
  return undefined
}
