const { PluginTypeLoaderBase } = require('plugnplay');
const rule_names = new Set();
module.exports = class extends PluginTypeLoaderBase {
    exportSync(opts) {
        return {
            props: ['bnf', 'stringify', '_main_rule_name'],
            validate: (exports) => {
                for (const f of ['bnf', 'stringify']) {
                    if (!exports[f]) {
                        throw new Error(`missing field ${f}`);
                    }
                }
                const rules = exports.bnf.split("\n");
                if (rules[0] === "") {
                    throw new Error("First line should be the main rule");
                }
                for (const rule of rules) {
                    if (rule === "") {
                        continue;
                    }
                    const name = rule.match(/^(\w+)\s*::=/);
                    if (!name) {
                        throw new Error(`invalid bnf rule: ${rule}`);
                    }
                    if (name[1].substr(0, 6) !== "MACRO_") {
                        throw new Error(`${name[1]} token name should start with "MACRO_"`);
                    }
                    if (rule_names.has(name[1])) {
                        throw new Error(`${name[1]} token already registered`);
                    }
                    rule_names.add(name[1]);
                }
                exports._main_rule_name = rules[0].match(/^(\w+)\s*::=/)[1];
            }
        }
    }
}