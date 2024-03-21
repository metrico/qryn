const { standardBuilder } = require('./shared')

module.exports = standardBuilder((sel, ctx) => {
  if (!ctx.limit) {
    return sel
  }
  return sel.limit(ctx.limit)
})
