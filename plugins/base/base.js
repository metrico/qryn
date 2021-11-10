const { PluginLoaderBase } = require('plugnplay')
module.exports = class extends PluginLoaderBase {
  exportSync () {
    const res = {
      validate: (plg) => {
        res.props = Object.keys(plg)
        return res.props
      }
    }
    return res
  }
}
