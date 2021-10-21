const {PluginLoaderBase} = require('plugnplay');
module.exports = class extends PluginLoaderBase {
    exportSync() {
        return {
            bnf: `MACRO_test_macro_fn ::= "extract" <OWSP> "(" <OWSP> <quoted_str1> <OWSP> "," <OWSP> <quoted_str2> <OWSP> "," <OWSP> <quoted_str3> <OWSP> "," <OWSP> <quoted_str4> <OWSP> ")"`,
            /**
             *
             * @param token {Token}
             * @returns {string}
             */
            stringify: (token) => {
                return `first_over_time({${token.Child('quoted_str1')}=${token.Child('quoted_str2').value}} | json | line_format "\"${token.Child('quoted_str2')}\":\"{{${token.Child('quoted_str4')}}}\""| unwrap _entry [5s])`;
            }
        };
    }
}
