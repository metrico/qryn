const { PluginLoaderBase } = require('plugnplay')
module.exports = class extends PluginLoaderBase {
  exportSync () {
    return {
      bnf: 'MACRO_test_macro_fn ::= "test_macro" <OWSP> "(" <OWSP> <quoted_str> <OWSP> ")"',
      /**
             *
             * @param token {Token}
             * @returns {string}
             */
      stringify: (token) => {
        return `{test_id=${token.Child('quoted_str').value}}`
      }
    }
  }
}
