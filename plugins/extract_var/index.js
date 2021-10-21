const {PluginLoaderBase} = require('plugnplay');
module.exports = class extends PluginLoaderBase {
    exportSync() {
        return {
            bnf: `MACRO_extract_var_fn ::= "extract" <OWSP> "(" <OWSP> <quoted_str> <OWSP> "," <OWSP> <quoted_str> <OWSP> "," <OWSP> <quoted_str> <OWSP> "," <OWSP> <quoted_str> <OWSP> ")"`,
            /**
             *
             * @param token {Token}
             * @returns {string}
             */
            stringify: (token) => {
                return `first_over_time({${token.Child('quoted_str')[0].value}=${token.Child('quoted_str')[1].value}} | json | line_format "\"${token.Child('quoted_str')[2].value}\":\"{{${token.Child('quoted_str')[3].value}}}\""| unwrap _entry [5s])`;
            }
        };
    }
}
