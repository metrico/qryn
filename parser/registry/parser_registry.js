const {Compiler} = require("bnf/Compiler");
const {_and} = require("./common");

module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "json": (token, query) => {
        if (!token.Children("parameter").length) {
            throw new Error("Not supported yet");
        }
        const compiler = new Compiler();
        compiler.AddLanguage(`
<SYNTAX> ::= first_part *(part)
<first_part> ::= 1*(<ALPHA> | "_" | <DIGITS>)
<part> ::= ("." <first_part>) | "[" <QLITERAL> "]" | "[" <DIGITS> "]"
        `, "json_param");
        const labels = token.Children("parameter").reduce((sum, p) => {
            const label = p.Child("label").value;
            let val = compiler.ParseScript(JSON.parse(p.Child("quoted_str").value))
            val = [
                val.rootToken.Child("first_part").value,
                ...val.rootToken.Children("part").map(t => t.value)
            ];
            sum[label] = val;
            return sum;
        }, {});
        let exprs = Object.entries(labels).map(lbl => {
            const path = lbl[1].map(path => {
                if (path.startsWith(".")) {
                    return `'${path.substring(1)}'`;
                }
                if (path.startsWith("[\"")) {
                    return `'${JSON.parse(path.substring(1, path.length - 1))}'`;
                }
                if (path.startsWith("[")) {
                    return path.substring(1, path.length - 1);
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
    },
    "logfmt": () => {},
    "regexp": () => {},
    "unpack": () => {}
}