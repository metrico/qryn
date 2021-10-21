const {PluginLoaderBase} = require('plugnplay');
module.exports = class extends PluginLoaderBase {
    exportSync() {
        return {
            bnf: `MACRO_extract_var_fn ::= "extract" <OWSP> "(" <OWSP> <label> <OWSP> "," <OWSP> <quoted_str> <OWSP> "," <OWSP> <label> <OWSP> "," <OWSP> <label> <OWSP> ")"`,
            /**
             *
             * @param token {Token}
             * @returns {string}
             */
            stringify: (token) => {
                return `first_over_time({${token.Children('label')[0].value}=${token.Children('quoted_str')[0].value}} | json | line_format "{\"${token.Children('label')[1].value}\":{{${token.Children('label')[2].value}}} }"| unwrap _entry [5s])`;
            }
        };
    }
}
