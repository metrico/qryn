# Qryn Plugins

* WORK IN PROGRESS!

Missing a LogQL function in Qryn? Extend functionality in in _no time_ using [Qryn Plugins](https://github.com/metrico/qryn/tree/master/plugins)
Need to alias a complex query? Use macros to turn complex queries into easy to use queries

## Overall plugin structure

Plugins are supported via plugnplay module https://github.com/e0ipso/plugnplay .
To create a plugin you have to create a nodejs project with subfolders for each plugin or add them into your Qryn plugins folder:
```
/
|- package.json
|- plugin_name_folder
|  |- plugnplay.yml
|  |- index.js
|- plugin_2_folder
   |- plugnplay.yml
   ...
```

## Different types of plugins

There is a number of different types of plugins supported by Qryn. Each type extends particular functionality:
- Log-range aggregator over unwrapped range: `unwrap_registry` type (vanilla LogQL example: avg_over_time)
- Custom macro function to wrap or shorten an existing request statement: `macros` type


## Plugin implementation

### plugnplay.yml file
In order to initialize the plugin we need the `plugnplay.yml` file:

```
id: derivative
name: Derivative Plugin
description: Plugin to test pluggable extensions
loader: derivative.js
type: unwrap_registry
```

- `id` of the plugin should be unique.
- `type` of the plugin should be `unwrap_registry`.
- `loader` field should specify the js file exporting the plugin loader class.

The js module specified in the `loader` field should export a class extending  `PluginLoaderBase` class from the
plugnplay package.

```
const {PluginLoaderBase} = require('plugnplay');
module.exports = `class extends PluginLoaderBase {
    exportSync() { return {...}; }
}
```

The exporting class should implement one function: `exportSync() {...}`.
The `exportSync` function should return an object representing API different for each type of plugin.

Finally, you have to add the path to your plugin root folder to the env variable `PLUGINS_PATH`.
Different paths should be separated by comma sign `,`.

## Unwrapped Range Aggregation (unwrap_registry)

In this example we will add a new unwrapped range aggregator `derivative`:

`derivative=(last_unwrapped_value_in_range - first_unwrapped_value_in_range) / (last_time_in_range - first_time_in_range)`

You need to init a plugin with the following loader:
```
const {PluginLoaderBase} = require('plugnplay');
module.exports = `class extends PluginLoaderBase {
    exportSync(api) {
        return {
            derivative = {
                run: () => {},
                approx: () => {}
            }
        }
    }
}
```
`exportSync` is a function returning an object with the function name as key and two methods: `run` and `approx`.

The `run` method is called every time new unwrapped value accepted by the stream processor. Its declaration is:
```
        /**
         *
         * @param sum {any} previous value for the current time bucket
         * @param val {{unwrapped: number}} current values
         * @param time {number} timestamp in ms for the current value
         * @returns {any}
         */
        const run = (sum, val, time) => {
            sum = sum || {};
            sum.first = sum && sum.first && time > sum.first.time ? sum.first : {time: time, val: val.unwrapped};
            sum.last = sum && sum.last && time < sum.last ? sum.last : {time: time, val: val.unwrapped};
            return sum;
        }
```

So the run function accepts the previous aggregated value. The initial value is 0.
The second is an object with current unwrapped value.
And the time when the unwrapped value appeared in the database.
The run function should return the new sum. Data immutability is preferred but optional.

The `approx` method is called for each bucket at the end of processing. Its declaration is:
```
        /**
         * @param sum {any} sum of the time bucket you have created during "run"
         * @returns {number}
         */
        const approx = (sum) => {
            return sum && sum.last && sum.first && sum.last.time > sum.first.time ?
                (sum.last.val - sum.first.val) / (sum.last.time - sum.first.time) * 1000 : 0;
        }
```
The only argument is the result of the latest `run` call for the bucket.
The function should return number as result of the operator calculation for the provided time bucket.

## Example

The full code of the `derivative` plugin:

plugnplay.yml
```
id: derivative
name: Derivative Plugin
description: Plugin to test pluggable extensions
loader: derivative.js
type: unwrap_registry
```

derivative.js:
```
const {PluginLoaderBase} = require('plugnplay');

module.exports = class extends PluginLoaderBase {
    exportSync(api) {
        return {
            derivative: {
                /**
                 *
                 * @param sum {any} previous value for the current time bucket
                 * @param val {{unwrapped: number}} current values
                 * @param time {number} timestamp in ms for the current value
                 * @returns {any}
                 */
                run: (sum, val, time) => {
                    sum = sum || {};
                    sum.first = sum && sum.first && time > sum.first.time ? sum.first : {
                        time: time,
                        val: val.unwrapped
                    };
                    sum.last = sum && sum.last && time < sum.last ? sum.last : {time: time, val: val.unwrapped};
                    return sum;
                },
                /**
                 * @param sum {any} sum of the time bucket you have created during "run"
                 * @returns {number}
                 */
                approx: (sum) => {
                    return sum && sum.last && sum.first && sum.last.time > sum.first.time ?
                        (sum.last.val - sum.first.val) / (sum.last.time - sum.first.time) * 1000 : 0;
                }
            }
        };
    }
}
```

## Macro plugin implementation (macros)

Qryn parses logql requests using the bnf package https://github.com/daKuleMune/nodebnf#readme

You can provide a custom bnf token representation and map it to a relevant logql request via a plugin with `macros`
type.

The raw ABNF description: https://github.com/metrico/qryn/blob/master/parser/logql.bnf .

If you are unfamiliar BNF rules, here is a good resource to get a quick introduction: http://www.cs.umsl.edu/~janikow/cs4280/bnf.pdf

### Custom BNF requirements

A bnf description in your plugin should follow the requirements:
- one bnf rule on a string
- no multiline rules
- no comments supported
- bnf rule name should start with MACRO_ prefix
- no bnf rule name collisions

### Plugin API
A plugin should export two fields:
```
const exports = {
    bnf: "... bnf rules ...",
    /**
     *
     * @param token {Token}
     * @returns {string}
     */
    stringify: (token) => {}
}
```
The `bnf` field should contain bnf rules.

The `stringify` function should convert a parsed query token into a legit logQL request.

### The `Token` type
Token type is a request parsed by the BNF package. It has the following fields:

|    Field    |   Header                                    |   Description    |
| ----------- | ------------------------------------------- | ---------------- |
|  value      | token.value:string                          | part of the request expression corresponding to the token |
|  Child      | token.Child(child_type: string): Token      | function returning the first token child with the specified type. |
|  Children   | token.Children(child_type: string): Token[] | function returning all the token children with the specified type. |

### Example
Let's review an example of macro translating `test_macro("val1")` to `{test_id="val1"}`

The `plugnplay.yml` file
```
id: test_macro
name: test macro
description: A macro to test
loader: index.js
type: macros
```

The BNF description of the macro: `MACRO_test_macro_fn ::= "test_macro" <OWSP> "(" <OWSP> <quoted_str> <OWSP> ")"`

The complete loader code:
```
const {PluginLoaderBase} = require('plugnplay');
module.exports = class extends PluginLoaderBase {
    exportSync() {
        return {
            bnf: `MACRO_test_macro_fn ::= "test_macro" <OWSP> "(" <OWSP> <quoted_str> <OWSP> ")"`,
            /**
             *
             * @param token {Token}
             * @returns {string}
             */
            stringify: (token) => {
                return `{test_id=${token.Child('quoted_str').value}}`;
            }
        };
    }
}
```

### Commonly used tokens defined by the core BNF

You can use the common rules already defined in the core BNF description.

The raw ABNF description with all the rules: https://github.com/metrico/qryn/blob/master/parser/logql.bnf .

The rules defined in the BNF package are here: https://github.com/daKuleMune/nodebnf#readme

Commonly used LogQL rules:

| Rule name | Example | Description |
| ------------------- | ------- | ----------- |
| log_stream_selector      | <code>{label1 = "val1", l2 =~ "v2"} &#124;~ "re1"</code> | log stream selector with label selectors and all pipeline operators
| log_stream_selector_rule | `label1 = "val1"`                                        | one label selector rule
| label                    | `label1`                                                 | label name
| operator                 | `= / != / =~ / !~`                                       | label selector operator
| quoted_str               | `"qstr\""`                                               | one properly quoted string
| line_filter_expression   | <code>&#124;~ "re1"</code>                               | one line filter expression
| line_filter_operator     | <code>&#124;= / &#124;= / !~ / != </code>                | string filter operator
| parser_expression        |  <code>&#124; json jlbl="l1[1].l2" </code>               | one parser expression
| label_filter_expression  | <code>&#124; jlbl = "val1" </code>                       | one label filter in the pipeline part
| line_format_expression   | <code>&#124; line_format "l1: {{label1}}" </code>        | line format expression
| labels_format_expression | <code>&#124; line_format lbl1="l1: {{label1}}" </code>   | label format expression
| log_range_aggregation    | `rate({label1="val1"} [1m])`                             | log range aggregation expression
| aggregation_operator     | `sum(rate({label1="val1"} [1m])) by (lbl1, lbl2)`        | aggregation operator expression
| unwrap_expression        | <code>{label1="val1"} &#124;~ "re1" &#124; unwrap lbl2 </code>                      | line selector with pipeline ending with the unwrap expression
| unwrap_function          | <code>rate(rate({label1="val1"} &#124; unwrap int_lbl2 [1m]) by (label3)</code>     | unwrapped log-range aggregation
| compared_agg_statement   | <code>rate(rate({label1="val1"} &#124; unwrap int_lbl2 [1m]) by (label3) > 5</code> | wrapped or unwrapped log-range aggregation comparef to a numeric const
