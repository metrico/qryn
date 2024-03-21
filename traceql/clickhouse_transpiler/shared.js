const Sql = require('@cloki/clickhouse-sql')
/**
 *
 * @param op {string}
 */
module.exports.getCompareFn = (op) => {
  switch (op) {
    case '=':
      return Sql.Eq
    case '>':
      return Sql.Gt
    case '<':
      return Sql.Lt
    case '>=':
      return Sql.Gte
    case '<=':
      return Sql.Lte
    case '!=':
      return Sql.Ne
  }
  throw new Error('not supported operator: ' + op)
}

module.exports.durationToNs = (duration) => {
  const measurements = {
    ns: 1,
    us: 1000,
    ms: 1000000,
    s: 1000000000,
    m: 1000000000 * 60,
    h: 1000000000 * 3600,
    d: 1000000000 * 3600 * 24
  }
  const durationRe = duration.match(/(\d+\.?\d*)(ns|us|ms|s|m|h|d)?/)
  if (!durationRe) {
    throw new Error('Invalid duration compare value')
  }
  return parseFloat(durationRe[1]) * measurements[durationRe[2].toLowerCase()]
}

module.exports.unquote = (val) => {
  if (val[0] === '"') {
    return JSON.parse(val)
  }
  if (val[0] === '`') {
    return val.substr(1, val.length - 2)
  }
  throw new Error('unquote not supported')
}

/**
 * @typedef {function(Context): Select} BuiltProcessFn
 */
/**
 * @param fn {ProcessFn}
 * @returns {{
 *   new(): {
 *     withMain(BuiltProcessFn): this,
 *     build(): BuiltProcessFn
 *   },
 *   prototype: {
 *     withMain(BuiltProcessFn): this,
 *     build(): BuiltProcessFn
 *   }}}
 */
module.exports.standardBuilder = (fn) => {
  return class {
    withMain (main) {
      this.main = main
      return this
    }

    build () {
      return (ctx) => {
        const sel = this.main ? this.main(ctx) : null
        return fn(sel, ctx)
      }
    }
  }
}
