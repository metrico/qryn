/**
 * @returns {{run: (function(*, number, number): *), approx: (function(*): number)}}
 */
module.exports.derivative = () => {
    return {
        /**
         *
         * @param sum {any} previous value for the current time bucket
         * @param val {{unwrapped: number}} current values
         * @param time {number} timestamp in ms for the current value
         * @returns {any}
         */
        run: (sum, val, time) => {
            sum = sum || {};
            sum.first = sum && sum.first && time > sum.first.time ? sum.first : {time: time, val: val.unwrapped};
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

}