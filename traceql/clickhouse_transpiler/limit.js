/**
 *
 * @type {ProcessFn}
 */
module.exports.process = (sel, ctx) => {
  if (!ctx.limit) {
    return sel
  }
  return sel.limit(ctx.limit)
}
