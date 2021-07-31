const axios = require('axios');
const fs = require('fs');

/**
 * This is the Insert benchmark test.
 * In order to run the test you have to
 * - run clickhouse with appropriate databases
 * - provide all the needed environment for cLoki
 * - export LOKI_ENDPOINT=http://....loki endpoint...
 * - export SAME_DATA_BENCHMARK=1 env vars
 * - run jest
 */

const sameData = () => process.env.SAME_DATA_BENCHMARK === "1"

/**
 *
 * @param points {Object<string, {stream: Object<string, string>, values: [string, string]}>}
 * @param endpoint {string}
 * @returns {Promise<void>}
 */
const sendPoints = async (endpoint, points) => {
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

let l = null;

beforeAll( async () => {
    if (!sameData()) {
        return;
    }
    l = require("../cloki");
    await new Promise(f => setTimeout(f, 500));
});

afterAll(() => {
    sameData() && l.stop();
});

/**
 *
 * @param id {string}
 * @param frequencySec {number}
 * @param startMs {number}
 * @param endMs {number}
 * @param extraLabels {Object}
 * @param points {Object}
 */
const createPoints = (id, frequencySec, startMs, endMs, extraLabels, points) => {
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

it('should stream the same data to loki / cloki', async () => {
    const testId = Date.now().toString();
    console.log(testId);
    const start = Date.now() - 60 * 1000;
    const end = Date.now();
    let points = createPoints(testId, 1, start, end, {}, {});
    points = createPoints(testId, 2, start, end, {}, points);
    points = createPoints(testId, 4, start, end, {}, points);
    fs.writeFileSync('points.json', JSON.stringify({ streams: Object.values(points) }));
    await sendPoints('http://localhost:3100', points);
    await sendPoints(process.env.LOKI_ENDPOINT, points);
    await new Promise(f => setTimeout(f, 1000));
});