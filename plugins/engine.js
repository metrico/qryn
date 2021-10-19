const { PluginManager } = require('plugnplay');


const manager = new PluginManager({
    discovery: {
        rootPath: `/home/hromozeka/QXIP/+(cLoki/unwrap_registry|test_plugin)`
    }
});

const plugins = manager.discoverSync();

/**
 *
 * @type {Object<string, (int|Function)[][]>}
 */
const filters = {};
/**
 *
 * @param tag
 * @param filter
 * @param priority
 */
module.exports.addFilter = (tag, filter, priority) => {
    filters[tag] = filters[tag] || [];
    filters[tag].push([priority, filter]);
};
/**
 *
 * @param tag{string}
 * @param init {any}
 * @returns {any}
 */
module.exports.applyFilter = (tag, init) => {
    const fls = filters[tag] || [];
    fls.sort();
    return fls.reduce((sum, f) => f[1](sum), init);
};

const api = {
    addFilter: module.exports.addFilter,
    useFilter: module.exports.applyFilter
}

for (const plg of plugins) {
    manager.require(plg.id, api);
}