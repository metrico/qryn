module.exports.init = () => {
  require('./qryn_node')
}
module.exports.bun = () => {
  try {
    return Bun
  } catch (e) {
    return false
  }
}
