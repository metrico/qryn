const {Compiler} = require("bnf/Compiler");
const {_and} = require("../common");
const {DataStream} = require("scramjet");

/**
 *
 * @type {function(Token): Object | undefined}
 */
const get_labels = (() => {
    const compiler = new Compiler();
    compiler.AddLanguage(`
<SYNTAX> ::= first_part *(part)
<first_part> ::= 1*(<ALPHA> | "_" | <DIGITS>)
<part> ::= ("." <first_part>) | "[" <QLITERAL> "]" | "[" <DIGITS> "]"
        `, "json_param");
    /**
     * @param token {Token}
     * @returns {Object | undefined}
     */
    return (token) => {
        if (!token.Children("parameter").length) {
            return undefined;
        }
        return token.Children("parameter").reduce((sum, p) => {
            const label = p.Child("label").value;
            let val = compiler.ParseScript(JSON.parse(p.Child("quoted_str").value))
            val = [
                val.rootToken.Child("first_part").value,
                ...val.rootToken.Children("part").map(t => t.value)
            ];
            sum[label] = val;
            return sum;
        }, {});
    }
})();

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.via_clickhouse_query = (token, query) => {
    const labels = get_labels(token);
    let exprs = Object.entries(labels).map(lbl => {
        const path = lbl[1].map(path => {
            if (path.startsWith(".")) {
                return `'${path.substring(1)}'`;
            }
            if (path.startsWith("[\"")) {
                return `'${JSON.parse(path.substring(1, path.length - 1))}'`;
            }
            if (path.startsWith("[")) {
                return (parseInt(path.substring(1, path.length - 1))+1).toString();
            }
            return `'${path}'`;
        });
        const expr = `if(JSONType(samples.string, ${path.join(",")}) == 'String', `+
            `JSONExtractString(samples.string, ${path.join(",")}), `+
            `JSONExtractRaw(samples.string, ${path.join(",")}))`
        return `('${lbl[0]}', ${expr})`;
    });
    exprs = "arrayFilter((x) -> x.2 != '', [" + exprs.join(",") + "])";
    return _and({
        ...query,
        select: [...query.select.filter(f => !f.endsWith("as extra_labels")), `${exprs} as extra_labels` ]
    }, ['isValidJSON(samples.string)']);
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.via_stream = (token, query) => {
    const labels = get_labels(token);

    /**
     *
     * @param {any} obj
     * @param {string} prefix
     * @returns {string|{}|null}
     */
    const obj_to_labels = (obj, prefix) => {
        if (Array.isArray(obj) ||
            obj === null
        ) {
            return null;
        }
        if (typeof obj === "object") {
            let res = {};
            for (const k of Object.keys(obj)) {
                const label = prefix + (prefix ? "_" : "") + (k.replace(/[^a-zA-Z0-9_]/g, '_'));
                const val = obj_to_labels(obj[k], label);
                if (typeof val === 'object') {
                    res = {...res, ...val};
                    continue;
                }
                res[label] = val;
            }
            return res;
        }
        return obj.toString();
    };

    /**
     *
     * @param {Object} obj
     * @param {String[]} path
     */
    const extract_label = (obj, path) => {
        let res = obj;
        for (const p in path) {
            if (!res[p]) {
                return undefined;
            }
            res = res[p];
        }
        if (typeof res === 'object' || Array.isArray(res)) {
            return JSON.stringify(res);
        }
        return res.toString();
    };

    /**
     *
     * @param {Object} obj
     * @param {Object} labels
     */
    const extract_labels = (obj, labels) => {
        let res = {};
        for (const l in Object.keys(labels)) {
            res[l] = extract_label(obj, labels[l]);
        }
        return res;
    };

    /**
     *
     * @param {DataStream} stream
     * @return {DataStream}
     */
    const stream = (stream) => {
        return stream.map((e) => {
            try {
                const oString = JSON.parse(e.string);
                const extra_labels = labels ? extract_labels(oString, labels) : obj_to_labels(oString, "");
                return { ...e, labels: {...e.labels, ...extra_labels}};
            } catch (err) {
                return undefined;
            }
        });
    };
    return {
        ...query,
        stream: [...(query.stream ? query.stream : []), stream]
    };
}
