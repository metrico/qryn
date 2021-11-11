const Base = require('../base')
module.exports = class extends Base {
  exportSync () {
    const res = {
      validate: (plg) => {
        res.props = Object.entries(plg).filter(e => e[1].map || e[1].remap).map(e => e[0])
        return res.props
      }
    }
    return res
  }
}
