const hb = require('handlebars');
require("handlebars-helpers")(['math', 'string'], {
    handlebars: hb
});

/**
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports = (token, query) => {
    const fmt = JSON.parse("\"" +  token.Child('quoted_str').value.replace(/(^"|^'|"$|'$)/g, "") + "\"");
    const processor = hb.compile(fmt);
    return {
        ...query,
        stream: [...(query.stream ? query.stream : []),
            /**
             *
             * @param s {DataStream}
             */
            (s) => s.map((e) => {
                if (!e.labels) {
                    return e;
                }
                try {
                    return {
                        ...e,
                        string: processor({...e.labels, _entry: e.string})
                    }
                } catch (err) {
                    return null;
                }
            }).filter(e => e)]
    };
};