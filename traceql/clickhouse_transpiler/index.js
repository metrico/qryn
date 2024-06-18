const AttrConditionPlanner = require('./attr_condition')
const AttrConditionEvalPlanner = require('./attr_condition_eval')
const InitIndexPlanner = require('./init')
const IndexGroupByPlanner = require('./group_by')
const AggregatorPlanner = require('./aggregator')
const IndexLimitPlanner = require('./limit')
const TracesDataPlanner = require('./traces_data')
const { th } = require('date-fns/locale')

/**
 * @param script {Token}
 */
module.exports.transpile = (script) => {
  return new module.exports.Planner(script).plan()
}

/**
 * @param script {Token}
 */
module.exports.evaluateCmpl = (script) => {
  return new module.exports.Planner(script).planEval()
}

module.exports.Planner = class Planner {
  /**
   *
   * @param script {Token}
   */
  constructor (script) {
    this.script = script
    this.cond = null
    this.terms = {}
    this.termIdx = []

    this.eval = null

    this.preCond = null
    this.preCondTerms = {}
    this.precondTermsIdx = []

    this.aggregatedAttr = ''
    this.cmpVal = ''

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
    if (this.preCond) {
      const preCond = (new AttrConditionPlanner())
        .withTerms(this.precondTermsIdx)
        .withConditions(this.preCond)
        .withMain((new InitIndexPlanner()).build())
      res = res.withPrecondition(preCond.build())
    }
    res = res.build()
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

  planEval () {
    this.check()
    this.analyze()
    const res = (new AttrConditionEvalPlanner())
      .withTerms(this.termIdx)
      .withConditions(this.cond)
      .withAggregatedAttr(this.aggregatedAttr)
      .withMain((new InitIndexPlanner()).build())
      .build()

    return res
  }

  setEvaluationResult (result) {
    this.eval = {}
    for (const row of result) {
      this.eval[row.cond] = row.count
    }
  }

  minify () {
    const subcost = {}
    for (let i = 0; i < this.termIdx.length; i++) {
      subcost[this.termIdx[i].value] = Object.entries(this.eval)
        .find(x => parseInt(x[0]) === i + 1)
      subcost[this.termIdx[i].value] = subcost[this.termIdx[i].value]
        ? parseInt(subcost[this.termIdx[i].value][1])
        : 0
    }
    if (!this.isDNF(this.cond)) {
      return this.estimateCost(this.cond, subcost)
    }
    this.preCond = this.getSimplePrecondition(this.cond, subcost)
    if (this.preCond) {
      this.extractTermsIdx(this.preCond, this.precondTermsIdx, this.preCondTerms)
    }

    return this.preCond
      ? this.estimateCost(this.preCond, subcost)
      : this.estimateCost(this.cond, subcost)
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
   * @param tree {{root: any}}
   * @param place {{ref: any}}
   */
  buildExpressionTree (token, tree, place) {
    if (token.name !== 'attr_selector_exp') {
      throw new Error('unsupported selector')
    }
    let leftHand = token.tokens[0]
    if (token.tokens[0].name === 'complex_head') {
      const newTree = { root: { ref: null } }
      this.buildExpressionTree(token.tokens[0].tokens.find(x => x.name === 'attr_selector_exp'),
        newTree,
        newTree.root
      )
      leftHand = newTree.root
    }
    const tail = token.tokens.find(x => x.name === 'tail')
    if (!tail) {
      // if we have `a`
      place.ref = leftHand
      return
    }
    const andOr = token.tokens.find(x => x.name === 'and_or').value
    const newPlace = { ref: null }
    switch (andOr) {
      case '&&':
        place.ref = ['&&', { ref: leftHand }, newPlace]
        this.buildExpressionTree(tail.tokens[0], tree, newPlace)
        return
      case '||':
        place.ref = leftHand
        tree.root = ['||', { ref: tree.root }, newPlace]
        this.buildExpressionTree(tail.tokens[0], tree, newPlace)
    }
  }

  /**
   *
   * @param t {{ref: any} | Token | Array}
   * @returns {Token | Array}
   */
  minimizeTree (t) {
    while (t.ref) {
      t = t.ref
    }
    if (!Array.isArray(t)) {
      return t
    }
    for (let i = t.length - 1; i > 0; i--) {
      t[i] = this.minimizeTree(t[i])
      if (Array.isArray(t[i]) && t[i][0] === t[0]) {
        t.splice(i, 1, ...t[i].slice(1))
      }
    }
    return t
  }

  /**
   * @param t {Token | Array}
   * @returns {boolean}
   */
  isDNF (t) {
    if (t.name) {
      return true
    }
    const fn = t[0]
    for (let i = 1; i < t.length; i++) {
      if (!this.isDNF(t[i])) {
        return false
      }
      if (Array.isArray(t[i]) && fn === '&&' && t[i][0] === '||') {
        return false
      }
    }
    return true
  }

  /**
   *
   * @param t {Token | Array}
   * @param subcosts {{[key: string]: number}}
   * @returns number
   */
  estimateCost (t, subcosts) {
    if (t.name) {
      return subcosts[t.value]
    }
    const fn = t[0]
    const costs = t.slice(1).map(x => this.estimateCost(x, subcosts))
    switch (fn) {
      case '&&':
        return Math.min(...costs)
      case '||':
        return costs.reduce((a, b) => a + b)
    }
    throw new Error('unsupported function')
  }

  /**
   *
   * @param t {Token | Array}
   * @param subcosts {{[key: string]: number}}
   */
  getSimplePrecondition (t, subcosts) {
    if (!this.isDNF(t)) {
      return null
    }
    if (t.name) {
      return subcosts[t.value] < 10000000 ? t : null
    }
    const fn = t[0]
    const self = this
    const simplify = x => x.length === 2 ? x[1] : x
    if (fn === '&&') {
      const res = t.slice(1).filter(x => self.estimateCost(x, subcosts) < 10000000)
      return res.length > 0 ? simplify(['&&', ...res]) : null
    }
    if (fn === '||') {
      const res = t.slice(1).map(x => self.getSimplePrecondition(x, subcosts)).filter(x => x)
      return res.length === t.length - 1 ? simplify(['||', ...res]) : null
    }
    throw new Error('unsupported function')
  }

  /**
   *
   * @param token {Token}
   */
  analyzeCond (token) {
    const tree = { root: { ref: null } }
    this.buildExpressionTree(token, tree, tree.root)
    tree.root = this.minimizeTree(tree.root)
    this.extractTermsIdx(tree.root, this.termIdx, this.terms)
    return tree.root
  }

  extractTermsIdx (t, termIdx, terms) {
    const self = this
    if (t.name) {
      if (!terms[t.value]) {
        termIdx.push(t)
        terms[t.value] = termIdx.length
        t.termIdx = termIdx.length - 1
      } else {
        t.termIdx = terms[t.value] - 1
      }
      return
    }
    if (Array.isArray(t)) {
      t.forEach(x => self.extractTermsIdx(x, termIdx, terms))
    }
  }

  analyzeAgg () {
    const agg = this.script.Child('aggregator')
    if (!agg) {
      return
    }
    if (['count', 'sum', 'min', 'max', 'avg'].indexOf(agg.Child('fn').value) < 0) {
      return
    }
    this.aggFn = agg.Child('fn').value
    const labelName = agg.Child('attr').Child('label_name')
    this.aggregatedAttr = labelName ? labelName.value : ''
    this.cmpVal = agg.Child('cmp_val').value
  }
}
