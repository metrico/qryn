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
        if (!token.Children("parameter").length || (query.stream && query.stream.length)) {
            return json.via_stream(token, query);
        }
        return json.via_clickhouse_query(token, query);
    },
    "logfmt": _i,

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "regexp": (token, query) => {
        const re = new RegExp(JSON.parse(token.Child("parameter").value));
        const getLabels = (m) => {
            return m && m.groups ? m.groups : {};
        }
        return {
            ...query,
            stream: [...(query.stream || []),
                (s) => s.map(e => {
                    return e.labels ? {
                        ...e,
                        labels: {
                            ...e.labels,
                            ...getLabels(e.string.match(re))
                        }
                    } : e;
                })
            ]
        };
    }
}