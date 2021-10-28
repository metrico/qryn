const {PluginLoaderBase} = require("plugnplay");
module.exports = class extends PluginLoaderBase {
    exportSync() {
        let res =  { validate: (plg) => res.props = Object.keys(plg) };
        return res;
    }
}