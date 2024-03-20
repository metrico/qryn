const Sql = require('@cloki/clickhouse-sql')
const { getCompareFn, durationToNs } = require('./shared')

module.exports = class Builder {
  constructor () {
    this.fn = ''
    this.attr = ''
    this.compareFn = ''
    this.compareVal = ''
  }

  /**
   *
   * @param fn {string}
   * @returns {Builder}
   */
  withFn (fn) {
    this.fn = fn
    return this
  }

  /**
   *
   * @param attr {string}
   * @returns {Builder}
   */
  withAttr (attr) {
    this.attr = attr
    return this
  }

  /**
   *
   * @param fn {string}
   * @returns {Builder}
   */
  withCompareFn (fn) {
    this.compareFn = fn
    return this
  }

  /**
   *
   * @param val {string}
   * @returns {Builder}
   */
  withCompareVal (val) {
    this.compareVal = val
    return this
  }

  /**
   * @returns {ProcessFn}
   */
  build () {
    const self = this
    /** @type {ProcessFn} */
    const res = (sel, ctx) => {
      const fCmpVal = self.cmpVal()
      const agg = self.aggregator()
      const compareFn = getCompareFn(self.compareFn)
      return sel.having(compareFn(agg, Sql.val(fCmpVal)))
    }
    return res
  }

  cmpVal () {
    if (this.attr === 'duration') {
      return durationToNs(this.compareVal)
    }
    return parseFloat(this.compareVal)
  }

  aggregator () {
    switch (this.fn) {
      case 'count':
        return new Sql.Raw('toFloat64(count(distinct index_search.span_id))')
      case 'avg':
        return new Sql.Raw('avgIf(agg_val, isNotNull(agg_val))')
      case 'max':
        return new Sql.Raw('maxIf(agg_val, isNotNull(agg_val))')
      case 'min':
        return new Sql.Raw('minIf(agg_val, isNotNull(agg_val))')
      case 'sum':
        return new Sql.Raw('sumIf(agg_val, isNotNull(agg_val))')
    }
  }
}
