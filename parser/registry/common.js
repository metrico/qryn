const glob = require("glob");

/**
 * @param query {registry_types.Request}
 * @param clauses {string[]}
 * @returns {registry_types.Request}
 */
module.exports._and = (query, clauses) => {
    query = {...query};
    if (!query.where) {
        query.where = ['AND'];
    } else if (query.where[0] !== 'AND') {
        query.where = ['AND', query.where];
    } else {
        query.where = [...query.where];
    }
    query.where.push.apply(query.where, clauses);
    return query;
}

/**
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.querySelectorPostProcess = (query) => {
    return query;
}

/**
 *
 * @param token {Token}
 * @returns {string}
 */
module.exports.unquote_token = (token) => {
    let value = token.Child('quoted_str').value;
    value = `"${value.substr(1, value.length - 2)}"`;
    return JSON.parse(value);
}

/**
 *
 * @param duration_str {string}
 * @returns {number}
 */
module.exports.durationToMs = (duration_str) => {
    const durations = {
        "ns": 1/1000000,
        "us": 1/1000,
        "ms": 1,
        "s": 1000,
        "m": 60000,
        "h": 60000 * 60
    };
    for (const k of Object.keys(durations)) {
        const m = duration_str.match(new RegExp(`^([0-9][.0-9]*)${k}$`));
        if (m) {
            return parseInt(m[1]) * durations[k];
        }
    }
    throw new Error("Unsupported duration");
}

/**
 *
 * @param s {DataStream}
 * @param fn
 * @returns {DataStream}
 */
module.exports.map = (s, fn) => s.map((e) => {
    return new Promise(f => {
        setImmediate(() => {
            f(fn(e));
        });
    });
});

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {number}
 */
module.exports.getDuration = (token, query) => {
    const duration = module.exports.durationToMs(token.Child('duration_value').value);
    return duration; //Math.max(duration, query.ctx && query.ctx.step ? query.ctx.step : 1000);
}

/**
 *
 * @param eof {any}
 * @returns boolean
 */
module.exports.isEOF = (eof) => eof.EOF;

module.exports.getPlugins = (path, cb) => {
    let plugins = {};
    for (let file of glob.sync(path + "/*.js")) {
        const mod = require(file);
        for (let fn of Object.keys(mod)) {
            plugins[fn] = cb ? cb(mod[fn]()) : mod[fn]();
        }
    }
    return plugins;
}