const axios = require("axios");
/**
 *
 * @param id {string}
 * @param frequencySec {number}
 * @param startMs {number}
 * @param endMs {number}
 * @param extraLabels {Object}
 * @param points {Object}
 */
module.exports.createPoints = (id, frequencySec, startMs, endMs, extraLabels, points) => {
    const streams = {
        'test_id': id,
        'freq': frequencySec.toString(),
        ...extraLabels
    };
    const values = new Array(Math.floor((endMs - startMs) / frequencySec / 1000)).fill(0)
        .map((v, i) => [ ((startMs + frequencySec * i * 1000) * 1000000).toString(), `FREQ_TEST_${i}` ]);
    points = {...points};
    points[JSON.stringify(streams)] = {
        stream: streams,
        values: values
    };
    return points;
}

/**
 *
 * @param points {Object<string, {stream: Object<string, string>, values: [string, string]}>}
 * @param endpoint {string}
 * @returns {Promise<void>}
 */
module.exports.sendPoints = async (endpoint, points) => {
    try {
        console.log(`${endpoint}/loki/api/v1/push`);
        await axios.post(`${endpoint}/loki/api/v1/push`, {
            streams: Object.values(points)
        }, {
            headers:{"Content-Type": "application/json"}
        });
    } catch (e) {
        console.log(e.response);
        throw e;
    }
};
