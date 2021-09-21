const json = require("./json");

const _i = () => {throw new Error("Not implemented")}

module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "json": (token, query) => {
        if (!token.Children("parameter").length) {
            return json.via_stream(token, query);
        }
        return json.via_clickhouse_query(token, query);
    },
    "logfmt": _i,
    "regexp": _i
}