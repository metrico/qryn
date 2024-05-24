const attrCondition = require('./attr_condition')
const {bitSet} = require('./shared')
const Sql = require('@cloki/clickhouse-sql')
module.exports = class Builder extends attrCondition {
  build () {
    const self = this
    const superBuild = super.build()
    /** @type {BuiltProcessFn} */
    const res = (ctx) => {
      const sel = superBuild(ctx)
      sel.having_conditions = []
      sel.aggregations = [bitSet(self.sqlConditions)]
      sel.select_list = [[new Sql.Raw('count()'), 'count']]
      sel.order_expressions = []
      return sel
    }
    return res
  }
}
