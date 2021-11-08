const { PluginLoaderBase } = require('plugnplay')
module.exports = class extends PluginLoaderBase {
  exportSync () {
    return {
      bnf: 'MACRO_extract_var_fn ::= "extract" <OWSP> "(" <OWSP> <label> <OWSP> "," <OWSP> <label> <OWSP> "," <OWSP> <label> <OWSP> "," <OWSP> <label> <OWSP> ")"',
      /**
             *
             * @param token {Token}
             * @returns {string}
             */
      stringify: (token) => {
        return `first_over_time({${token.Children('label')[0].value}="${token.Children('label')[1].value}"} | json ${token.Children('label')[2].value}="${token.Children('label')[3].value}" | unwrap ${token.Children('label')[2].value} [5s]) by (http)`
      }
    }
  }
}
