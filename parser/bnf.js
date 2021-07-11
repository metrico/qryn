const registry_names = [
    'high_level_aggregation_registry',
    'log_range_aggregation_registry',
    'number_operator_registry',
    'stream_selector_operator_registry',
    'line_filter_operator_registry',
    'parser_registry'
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
let compiler = new Compiler();
compiler.AddLanguage(bnf , "logql" );

//Hack to process well-escaped quoted strings.
if (!String.prototype.splice) {
    /**
     * {JSDoc}
     *
     * The splice() method changes the content of a string by removing a range of
     * characters and/or adding new characters.
     *
     * @this {String}
     * @param {number} start Index at which to start changing the string.
     * @param {number} delCount An integer indicating the number of old chars to remove.
     * @param {string} newSubStr The String that is spliced in.
     * @return {string} A new string with the spliced substring.
     */
    String.prototype.splice = function(start, delCount, newSubStr) {
        return this.slice(0, start) + newSubStr + this.slice(start + Math.abs(delCount));
    };
}
compiler._ParseScript = compiler.ParseScript;
/**
 *
 * @param script {string}
 * @param dataObject {any}
 * @param languageId {any}
 * @constructor
 */
compiler.ParseScript = (script, dataObject = {}, languageId = null) => {
    let state = 0;
    const states = {
        NON_QUOTED: 0,
        QUOTE_STARTED: 1,
        NON_QUOTED_SLASH: 2,
        QUOTED_SLASH: 3
    }
    let min = 0, max = 0, val = "", quot_start = "", values = [];
    const onSlash = () => {
        switch (state) {
            case states.NON_QUOTED:
                state = states.NON_QUOTED_SLASH;
                break;
            case states.NON_QUOTED_SLASH:
                state = states.NON_QUOTED;
                break;
            case states.QUOTED_SLASH:
                val += "\\";
                break;
            case states.QUOTE_STARTED:
                state = states.QUOTED_SLASH;
                break;
        }
    }
    const onQuote = (i,char) => {
        switch (state) {
            case states.NON_QUOTED:
                state = states.QUOTE_STARTED;
                quot_start = char;
                val = "";
                min = i;
                break;
            case states.NON_QUOTED_SLASH:
                state = states.NON_QUOTED;
                break;
            case states.QUOTE_STARTED:
                max = i;
                if (char === quot_start) {
                    values.push({start: min, end: max, val: val, id: values.length});
                } else {
                    val += char;
                }
                state=states.NON_QUOTED;
                break;
            case states.QUOTED_SLASH:
                val += "\\"+char;
                state=states.QUOTE_STARTED;
                break;
        }
    }
    const onChar = (i, char) => {
        switch (state) {
            case states.NON_QUOTED:
            case states.NON_QUOTED_SLASH:
                break;
            case states.QUOTE_STARTED:
                val += char;
                break;
            case states.QUOTED_SLASH:
                switch (char) {
                    case 'n':
                        val += "\n";
                        break;
                    case 't':
                        val += "\t";
                        break;
                    case '\\':
                        val += '\\';
                        break;
                    default:
                        throw new Error(`\\${char} is npt supported`);
                }
                break;
        }
    }
    for(let i = 0; i < script.length; ++i) {
        switch (script.charAt(i)) {
            case '\\':
                onSlash(i, script.charAt(i));
                break;
            case '"':
            case '`':
                onQuote(i, script.charAt(i));
                break;
            default:
                onChar(i, script.charAt(i));
                break;
        }
    }
    for (let quot of values.reverse()) {
        script = script.splice(quot.start+1, quot.end - quot.start - 1 , `quot_${quot.id}`);
    }
    values.reverse();
    const res = compiler._ParseScript(script, dataObject, languageId);
    res.rootToken.Children('QLITERAL').forEach(c => {
        const val = c.value;
        c.tokens = [];
        c._value = `"${values[parseInt(val.substring(6, val.length-1))].val}"`;
    });
    res.rootToken.Children('AQLITERAL').forEach(c => {
        const val = c.value;
        c.tokens = [];
        c._value = `\`${values[parseInt(val.substring(6, val.length-1))].val}\``;
    });
    return res;

}

module.exports = compiler;