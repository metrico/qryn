const {_and, unquote_token, querySelectorPostProcess} = require("./common");

/**
 *
 * @param token {Token}
 * @returns {string[]}
 */
const label_and_val = (token) => {
    const label = token.Child('label').value;
    return [label, unquote_token(token)];
}



module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!=": (token, query) => {
        const [label, value] = label_and_val(token);
        return querySelectorPostProcess(_and(query, [
            `JSONHas(labels, '${label}')`,
            `JSONExtractString(labels, '${label}') != '${value}'`
        ]));
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "=~": (token, query) => {
        const [label, value] = label_and_val(token);
        return querySelectorPostProcess(_and(query, [
            `JSONHas(labels, '${label}')`,
            `extractAllGroups(JSONExtractString(labels, '${label}'), '(${value})') != []`
        ]));
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!~": (token, query) => {
        const [label, value] = label_and_val(token);
        return querySelectorPostProcess(_and(query, [
            `JSONHas(labels, '${label}')`,
            `extractAllGroups(JSONExtractString(labels, '${label}'), '(${value})') == []`
        ]));
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "=": (token, query) => {
        const [label, value] = label_and_val(token);
        return querySelectorPostProcess(_and(query, [
            `JSONHas(labels, '${label}')`,
            `JSONExtractString(labels, '${label}') = '${value}'`
        ]));
    }
};