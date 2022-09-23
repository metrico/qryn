const { PluginTypeLoaderBase } = require('plugnplay')
module.exports = class extends PluginTypeLoaderBase {
  exportSync (opts) {
    return {
      props: ['check', 'process'],
      validate: (exports) => {
        return exports
      }
    }
  }
}
