const { getCompareFn, durationToNs, unquote, bitSet } = require('./shared')
const Sql = require('@cloki/clickhouse-sql')
module.exports = class Builder {
  constructor () {
    this.main = null
    this.precondition = null
    this.terms = []
    this.conds = null
    this.aggregatedAttr = ''

    this.sqlConditions = []
    this.isAliased = false
    this.alias = ''
    this.where = []
  }

  /**
   *
   * @param main {BuiltProcessFn}
   * @returns {Builder}
   */
  withMain (main) {
    this.main = main
    return this
  }

  /**
   * @param terms {[]}
   * @returns {Builder}
   */
  withTerms (terms) {
    this.terms = terms
    return this
  }

  /**
   * @param conds
   * @returns {Builder}
   */
  withConditions (conds) {
    this.conds = conds
    return this
  }

  /**
   *
   * @param aggregatedAttr {string}
   * @returns {Builder}
   */
  withAggregatedAttr (aggregatedAttr) {
    this.aggregatedAttr = aggregatedAttr
    return this
  }

  withPrecondition (precondition) {
    this.precondition = precondition
    return this
  }

  /**
   * @returns {ProcessFn}
   */
  build () {
    const self = this
    /** @type {BuiltProcessFn} */
    const res = (ctx) => {
      const sel = self.main(ctx)
      const withPreconditionSel = self.precondition
        ? new Sql.With('precond', self.buildPrecondition(ctx))
        : null
      self.alias = 'bsCond'
      for (const term of self.terms) {
        const sqlTerm = self.getTerm(term)
        self.sqlConditions.push(sqlTerm)
        if (!term.Child('label_name').value.match(/^(\.|span\.|resource\.|name)/)) {
          continue
        }
        self.where.push(sqlTerm)
      }
      const having = self.getCond(self.conds)
      self.aggregator(sel)
      sel.conditions = Sql.And(sel.conditions, Sql.Or(...self.where))
      if (Array.isArray(ctx.randomFilter) && Array.isArray(ctx.cachedTraceIds) && ctx.cachedTraceIds.length > 0) {
        sel.conditions = Sql.And(
          sel.conditions,
          Sql.Or(
            Sql.Eq(new Sql.Raw(`cityHash64(trace_id) % ${ctx.randomFilter[0]}`), Sql.val(ctx.randomFilter[1])),
            new Sql.In('trace_id', 'in', ctx.cachedTraceIds.map(traceId => new Sql.Raw(`unhex('${traceId}')`)))
          ))
      } else if (Array.isArray(ctx.randomFilter)) {
        sel.conditions = Sql.And(
          sel.conditions,
          Sql.Eq(new Sql.Raw(`cityHash64(trace_id) % ${ctx.randomFilter[0]}`), Sql.val(ctx.randomFilter[1])))
      }
      if (withPreconditionSel) {
        sel.with(withPreconditionSel)
        sel.conditions = Sql.And(
          sel.conditions,
          new Sql.In(new Sql.Raw('(trace_id, span_id)'), 'in', new Sql.WithReference(withPreconditionSel)))
      }
      sel.having(having)
      return sel
    }
    return res
  }

  buildPrecondition (ctx) {
    if (!this.precondition) {
      return null
    }
    const sel = this.precondition(ctx)
    sel.select_list = sel.select_list.filter(x => Array.isArray(x) && (x[1] === 'trace_id' || x[1] === 'span_id'))
    sel.order_expressions = []
    return sel
  }

  /**
   * @typedef {{simpleIdx: number, op: string, complex: [Condition]}} Condition
   */
  /**
   * @param c {Token || [any]}
   */
  getCond (c) {
    if (c.name) {
      let left = new Sql.Raw(this.alias)
      if (!this.isAliased) {
        left = groupBitOr(bitSet(this.sqlConditions), this.alias)
      }
      const termIdx = this.terms.findIndex(x => x.value === c.value)
      return Sql.Ne(bitAnd(left, new Sql.Raw((BigInt(1) << BigInt(termIdx)).toString())), Sql.val(0))
    }
    const op = c[0]
    const subs = c.slice(1).map(x => this.getCond(x))
    switch (op) {
      case '&&':
        return Sql.And(...subs)
      case '||':
        return Sql.Or(...subs)
    }
    throw new Error(`unsupported condition operator ${c.op}`)
  }

  /**
   * @param sel {Select}
   */
  aggregator (sel) {
    if (!this.aggregatedAttr) {
      return
    }

    if (this.aggregatedAttr === 'duration') {
      sel.select([new Sql.Raw('toFloat64(any(traces_idx.duration))'), 'agg_val'])
      return
    }

    if (this.aggregatedAttr.match(/^span./)) {
      this.aggregatedAttr = this.aggregatedAttr.substr(5)
    }
    if (this.aggregatedAttr.match(/^resource\./)) {
      this.aggregatedAttr = this.aggregatedAttr.substr(9)
    }
    if (this.aggregatedAttr.match(/^\./)) {
      this.aggregatedAttr = this.aggregatedAttr.substr(1)
    }
    sel.select([sqlAttrValue(this.aggregatedAttr), 'agg_val'])
    this.where.push(Sql.Eq(new Sql.Raw('key'), Sql.val(this.aggregatedAttr)))
  }

  getTerm (term) {
    let key = term.Child('label_name').value
    if (key.match(/^span\./)) {
      key = key.substr(5)
    } else if (key.match(/^resource\./)) {
      key = key.substr(9)
    } else if (key.match(/^\./)) {
      key = key.substr(1)
    } else {
      switch (key) {
        case 'duration':
          return this.getDurationCondition(key, term)
        case 'name':
          key = 'name'
          break
        default:
          throw new Error(`unsupported attribute ${key}`)
      }
    }
    if (term.Child('quoted_str')) {
      return this.getStrCondition(key, term)
    } else if (term.Child('number')) {
      return this.getNumberCondition(key, term)
    }
    throw new Error(`unsupported term statement ${term.value}`)
  }

  getDurationCondition (key, term) {
    const fVal = durationToNs(term.Child('value').value)
    const fn = getCompareFn(term.Child('op').value)
    return fn(new Sql.Raw('traces_idx.duration'), Math.floor(fVal))
  }

  getStrCondition (key, term) {
    const strVal = this.getString(term)
    switch (term.Child('op').value) {
      case '=':
        return Sql.And(
          Sql.Eq(new Sql.Raw('key'), Sql.val(key)),
          Sql.Eq(new Sql.Raw('val'), Sql.val(strVal))
        )
      case '!=':
        return Sql.And(
          Sql.Eq(new Sql.Raw('key'), Sql.val(key)),
          Sql.Ne(new Sql.Raw('val'), Sql.val(strVal))
        )
      case '=~':
        return Sql.And(
          Sql.Eq(new Sql.Raw('key'), Sql.val(key)),
          Sql.Eq(new Sql.Raw(`match(val, ${Sql.quoteVal(strVal)})`), 1)
        )
      case '!~':
        return Sql.And(
          Sql.Eq(new Sql.Raw('key'), Sql.val(key)),
          Sql.Ne(new Sql.Raw(`match(val, ${Sql.quoteVal(strVal)})`), 1)
        )
    }
    throw new Error(`unsupported term statement ${term.value}`)
  }

  getNumberCondition (key, term) {
    const fn = getCompareFn(term.Child('op').value)
    if (!term.Child('value').value.match(/^\d+.?\d*$/)) {
      throw new Error(`invalid value in ${term.value}`)
    }
    const fVal = parseFloat(term.Child('value').value)
    return Sql.And(
      Sql.Eq('key', Sql.val(key)),
      Sql.Eq(new Sql.Raw('isNotNull(toFloat64OrNull(val))'), 1),
      fn(new Sql.Raw('toFloat64OrZero(val)'), fVal)
    )
  }

  getString (term) {
    if (term.Child('quoted_str').value) {
      return unquote(term.Child('quoted_str').value)
    }
    if (term.Child('number').value) {
      return term.Child('number').value
    }
    throw new Error(`unsupported term statement ${term.value}`)
  }
}

/**
 *
 * @param left
 * @param right
 * @returns {SQLObject}
 */
function bitAnd (left, right) {
  const res = new Sql.Raw('')
  res.toString = () => {
    const strLeft = left.toString()
    const strRight = right.toString()
    return `bitAnd(${strLeft}, ${strRight})`
  }
  return res
}

/**
 *
 * @param left
 * @param alias
 * @returns {SQLObject}
 */
function groupBitOr (left, alias) {
  const res = new Sql.Raw('')
  res.toString = () => {
    const strLeft = left.toString()
    if (alias) {
      return `groupBitOr(${strLeft}) as ${alias}`
    }
    return `groupBitOr(${strLeft})`
  }
  return res
}

/**
 *
 * @param attr {string}
 * @returns {SQLObject}
 */
function sqlAttrValue (attr) {
  const res = new Sql.Raw('')
  res.toString = () => {
    const strAttr = Sql.quoteVal(attr)
    return `anyIf(toFloat64OrNull(val), key == ${strAttr})`
  }
  return res
}

/**
 * type sqlAttrValue struct {
 *  attr string
 * }
 *
 * func (s *sqlAttrValue) String(ctx *sql.Ctx, options ...int) (string, error) {
 *  attr, err := sql.NewStringVal(s.attr).String(ctx, options...)
 *  if err != nil {
 *    return "", err
 *  }
 *
 *  return fmt.Sprintf("anyIf(toFloat64OrNull(val), key == %s)", attr), nil
 * }
 */