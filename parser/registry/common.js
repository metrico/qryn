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