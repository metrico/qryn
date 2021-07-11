const {_and, unquote_token} = require("./common");
const _i = () => { throw new Error('Not implemented'); };
module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "|=": (token, query) => {
        const val = unquote_token(token);
        return _and(query, [
            `position(string, '${val}') != 0`
        ]);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "|~": (token, query) => {
        const val = unquote_token(token);
        return _and(query, [
            `extractAllGroups(string, '(${val})') != []`
        ]);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!=": (token, query) => {
        const val = unquote_token(token);
        return _and(query, [
            `position(string, '${val}') == 0`
        ]);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!~": (token, query) => {
        const val = unquote_token(token);
        return _and(query, [
            `extractAllGroups(string, '(${val})') == []`
        ]);
    }
};