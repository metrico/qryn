const { PluginManager } = require('plugnplay')

const rootPath = !process.env.PLUGINS_PATH
  ? __dirname
  : `{${__dirname},${process.env.PLUGINS_PATH}}`

const manager = new PluginManager({
  discovery: {
    rootPath: rootPath,
    allowsContributed: false
  }
})

const plugins = manager.discoverSync()

for (const plg of plugins) {
  manager.require(plg.id)
}

module.exports.getPlg = (options) => {
  if (options.id) {
    return [...plugins.values()].some(p => p.id === options.id) ? manager.require(options.id).exports : null
  }
  if (options.type) {
    const res = {}
    for (const p of plugins) {
      if (p.type === options.type) {
        res[p.id] = manager.require(p.id).exports
      }
    }
    return res
  }
}
