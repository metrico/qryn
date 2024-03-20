const { getCompareFn, durationToNs, unquote } = require('./shared')
const Sql = require('@cloki/clickhouse-sql')
module.exports = class Builder {
  constructor () {
    this.terms = []
    this.conds = null
    this.aggregatedAttr = ''

    this.sqlConditions = []
    this.isAliased = false
    this.alias = ''
    this.where = []
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

  /**
   * @returns {ProcessFn}
   */
  build () {
    const self = this
    /** @type {ProcessFn} */
    const res = (sel, ctx) => {
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
      sel.having_conditions = Sql.And(sel.having_conditions, having)
      return sel
    }
    return res
  }

  /**
   * @typedef {{simpleIdx: number, op: string, comlex: [Condition]}} Condition
   */
  /**
   * @param c {Condition}
   */
  getCond (c) {
    if (c.simpleIdx === -1) {
      const subs = []
      for (const s of c.comlex) {
        subs.push(this.getCond(s))
      }
      switch (c.op) {
        case '&&':
          return Sql.And(...subs)
        case '||':
          return Sql.Or(...subs)
      }
      throw new Error(`unsupported condition operator ${c.op}`)
    }
    let left = new Sql.Raw(this.alias)
    if (!this.isAliased) {
      left = groupBitOr(bitSet(this.sqlConditions), this.alias)
    }
    return Sql.Ne(bitAnd(left, Sql.val(c.simpleIdx)), Sql.val(0))
  }

  /**
   * @param sel {Select}
   */
  aggregator (sel) {
    if (!this.aggregatedAttr) {
      return
    }

    const s = sel.select()
    if (this.aggregatedAttr === 'duration') {
      s.push([new Sql.Raw('toFloat64(duration)'), 'agg_val'])
      sel.select(...s)
      return
    }

    if (this.aggregatedAttr.match(/^span./)) {
      this.aggregatedAttr = this.aggregatedAttr.substr(5)
    }
    if (this.aggregatedAttr.match(/^resource\./)) {
      this.aggregatedAttr = this.aggregatedAttr.substr(9)
    }
    if (this.aggregatedAttr.match(/^\.*/)) {
      this.aggregatedAttr = this.aggregatedAttr.substr(1)
    }
    s.push([sqlAttrValue(this.aggregatedAttr), 'agg_val'])
    sel.select(...s)
    this.where.push(Sql.Eq(new Sql.Raw('key'), Sql.val(this.aggregatedAttr)))
  }

  getTerm (term) {
    let key = term.Child('label_name').value
    if (key.match(/^span\./)) {
      key = key.substr(5)
    } else if (key.match(/^resource\./)) {
      key = key.substr(9)
    } else if (key.match(/^.*/)) {
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
    const fn = getCompareFn(term.Child('op'))
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
    const fn = getCompareFn(term.Child('op'))
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
 * @param terms
 * @returns {SQLObject}
 */
function bitSet (terms) {
  const res = new Sql.Raw('')
  res.terms = terms
  res.toString = () => {
    return terms.map((t, i) => `bitShiftLeft(toUint64(${t.toString()}), ${i})`).join('+')
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