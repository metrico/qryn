const AttrConditionPlanner = require('./attr_condition')
const InitIndexPlanner = require('./init')
const IndexGroupByPlanner = require('./group_by')
const AggregatorPlanner = require('./aggregator')
const IndexLimitPlanner = require('./limit')
const TracesDataPlanner = require('./traces_data')

/**
 * @param script {Token}
 */
module.exports = (script) => {
  return new Planner(script).plan()
}

class Planner {
  /**
   *
   * @param script {Token}
   */
  constructor (script) {
    this.script = script
    this.termIdx = []
    this.cond = null
    this.aggregatedAttr = ''
    this.cmpVal = ''
    this.terms = {}
    this.aggFn = ''
  }

  plan () {
    this.check()
    this.analyze()
    let res = (new AttrConditionPlanner())
      .withTerms(this.termIdx)
      .withConditions(this.cond)
      .withAggregatedAttr(this.aggregatedAttr)
      .withMain((new InitIndexPlanner()).build())
      .build()
    res = (new IndexGroupByPlanner()).withMain(res).build()
    if (this.aggFn) {
      res = (new AggregatorPlanner())
        .withFn(this.aggFn)
        .withAttr(this.aggregatedAttr)
        .withCompareFn(this.script.Child('cmp').value)
        .withCompareVal(this.script.Child('cmp_val').value)
        .withMain(res)
        .build()
    }
    res = (new IndexLimitPlanner()).withMain(res).build()
    res = (new TracesDataPlanner()).withMain(res).build()
    res = (new IndexLimitPlanner()).withMain(res).build()

    return res
  }

  check () {
    if (this.script.Children('SYNTAX').length > 1) {
      throw new Error('more than one selector is not supported')
    }
  }

  analyze () {
    this.terms = {}
    this.cond = this.analyzeCond(this.script.Child('attr_selector_exp'))
    this.analyzeAgg()
  }

  /**
   *
   * @param token {Token}
   */
  analyzeCond (token) {
    let res = {}
    const complexHead = token.tokens.find(x => x.name === 'complex_head')
    const simpleHead = token.tokens.find(x => x.name === 'attr_selector')
    if (complexHead) {
      res = this.analyzeCond(complexHead.tokens.find(x => x.name === 'attr_selector_exp'))
    } else if (simpleHead) {
      const term = simpleHead.value
      if (this.terms[term]) {
        res = { simpleIdx: this.terms[term] - 1, op: '', complex: [] }
      } else {
        this.termIdx.push(simpleHead)
        this.terms[term] = this.termIdx.length
        res = { simpleIdx: this.termIdx.length - 1, op: '', complex: [] }
      }
    }
    const tail = token.tokens.find(x => x.name === 'tail')
    if (tail) {
      res = {
        simpleIdx: -1,
        op: token.tokens.find(x => x.name === 'and_or').value,
        complex: [res, this.analyzeCond(tail.tokens.find(x => x.name === 'attr_selector_exp'))]
      }
    }
    return res
  }

  analyzeAgg () {
    const agg = this.script.Child('aggregator')
    if (!agg) {
      return
    }
    this.aggFn = agg.Child('fn').value
    const labelName = agg.Child('attr').Child('label_name')
    this.aggregatedAttr = labelName ? labelName.value : ''
    this.cmpVal = agg.Child('cmp_val').value
  }
}
