const optimizations = [require('./optimization_v3_2')]

module.exports = {
  /**
   *
   * @param token {Token}
   * @param fromNS {number}
   * @param toNS {number}
   * @param stepNS {number}
   */
  apply: (token, fromNS, toNS, stepNS) => {
    const optimization = optimizations.find((opt) => opt.isApplicable(token, fromNS / 1000000))
    if (optimization) {
      return optimization.apply(token, fromNS, toNS, stepNS)
    }
    return null
  }
}
