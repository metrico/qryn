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
