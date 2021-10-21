const {get_plg} = require("../plugins/engine");
const registry_names = [
    'high_level_aggregation_registry',
    'log_range_aggregation_registry',
    'number_operator_registry',
    'stream_selector_operator_registry',
    'line_filter_operator_registry',
    'parser_registry',
    'unwrap_registry'
];
const registries = registry_names.reduce((sum, n) => {
    sum[n] = require(`${__dirname}/registry/${n}`);
    return sum;
}, {})
const fs = require('fs');

const { Compiler } = require( "bnf/Compiler" );
const { Token } = require( "bnf/Token" );

Token.prototype.Children = function ( tokenType ){
    let tokens = [];
    for( let i = 0; i < this.tokens.length; i++ ){
        if( this.tokens[i].name === tokenType ){
            tokens.push(this.tokens[i]);
        }
        else{
            tokens = [...tokens, ...this.tokens[i].Children( tokenType )];
        }
    }

    return tokens;
}



let bnf = fs.readFileSync(__dirname + "/logql.bnf").toString();
for (const reg of Object.keys(registries)) {
    bnf = bnf.replace(`<${reg}>`, Object.keys(registries[reg]).map(n => `"${n}"`).join("|"));
}
const plugins = get_plg({type: 'macros'});
bnf += Object.values(plugins).map(p => p.bnf).join("\n") + "\n";
bnf += "user_macro ::=" + Object.values(plugins).map(p => p._main_rule_name).map(n => `<${n}>`).join('|') + "\n";

let compiler = new Compiler();
compiler.AddLanguage(bnf , "logql" );

module.exports = compiler;
