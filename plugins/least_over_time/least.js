const {PluginLoaderBase} = require('plugnplay');

module.exports = class extends PluginLoaderBase {
    exportSync(api) {
        return {
            least_over_time: {
                /**
                 *
                 * @param lowest {any} previous value for the current time bucket
                 * @param val {{unwrapped: number}} current values
                 * @param time {number} timestamp in ms for the current value
                 * @returns {any}
                 */
                run: (lowest, val, time) => {
                    console.log('test', typeof lowest, lowest);
                    console.log('val', typeof val, val);
                    console.log('time', typeof time, time);
                    if(lowest == 0 || val.unwrapped < lowest) {
                      lowest = val.unwrapped
                    }
                    return lowest;
                },
                /**
                 * @param lowest {any} lowest of the time bucket you have created during "run"
                 * @returns {number}
                 */
                approx: (lowest) => {
                    //console.log('lowest', typeof lowest, lowest)
                    return lowest
                }
            }
        };
    }
}
