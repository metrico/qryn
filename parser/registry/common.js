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