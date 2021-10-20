# cLoki Plugins

* WORK IN PROGRESS!

Missing a LogQL function in cLoki? Extend functionality in in _no time_ using [cLoki Plugins](https://github.com/lmangani/cLoki/tree/master/plugins)

## Overall plugin structure

Plugins are supported via plugnplay module https://github.com/e0ipso/plugnplay .
To create a plugin you have to do a nodejs project with subfolders for each plugin:
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

There is a number of different types of plugins supported by cLoki. Each type extends particular functionality:
- Log-range aggregator over unwrapped range: `unwrap_registry` type
- Custom macro function to shortcut an existing request statement: `macros` type


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