const {createPoints, sendPoints} = require("./common");
const axios = require("axios");
const e2e = () => process.env.INTEGRATION_E2E || process.env.INTEGRATION;
let l = null;

beforeAll(async () => {
    if (!e2e()) {
        return;
    }
    l = require("../cloki");
    await new Promise(f => setTimeout(f, 500));
    jest.setTimeout(300000);
});
afterAll(() => {
    l.stop();
});

it("e2e", async () => {
    if (!e2e()) {
        return;
    }
    const testID = Math.random() + '';
    console.log(testID);
    const start = Math.floor((Date.now() - 60 * 1000 * 10) / 60 / 1000) * 60 * 1000;
    const end = Math.floor(Date.now() / 60 / 1000) * 60 * 1000;
    let points = createPoints(testID, 0.5, start, end, {}, {});
    points = createPoints(testID, 1, start, end, {}, points);
    points = createPoints(testID, 2, start, end, {}, points);
    points = createPoints(testID, 4, start, end, {}, points);
    await sendPoints('http://localhost:3100', points);
    await new Promise(f => setTimeout(f, 4000));
    // ok limited res
    let resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}"}&start=${start}000000&end=${end}000000&step=2`
    );
    const adjustResult = (resp) => {
        resp.data.data.result = resp.data.data.result.map(stream => {
            expect(stream.stream.test_id).toEqual(testID);
            stream.stream.test_id = "TEST_ID";
            stream.values = stream.values.map(v => [v[0] - start * 1000000, v[1]]);
            return stream;
        });
    }
    const adjustMatrixResult = (resp) => {
        resp.data.data.result = resp.data.data.result.map(stream => {
            expect(stream.metric.test_id).toEqual(testID);
            stream.metric.test_id = "TEST_ID";
            stream.values = stream.values.map(v => [v[0] - Math.floor(start / 1000), v[1]]);
            return stream;
        });
    }
    adjustResult(resp);
    expect(resp.data).toMatchSnapshot();
    //empty res
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}"}&start=${start - 3600 * 1000}000000&end=${end - 3600 * 1000}000000&step=2`
    );
    adjustResult(resp);
    expect(resp.data).toMatchSnapshot();
    //two clauses
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}", freq="2"}&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp);
    expect(resp.data).toMatchSnapshot();
    //two clauses and filter
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=100&query=%7Btest_id%3D%22${testID}%22%2C%20freq%3D%222%22%7D%20%7C~%20%222%5B0-9%5D%24%22&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp);
    expect(resp.data).toMatchSnapshot();
    //aggregation
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=1788&query=rate(%7Btest_id%3D%22${testID}%22%2C%20freq%3D%222%22%7D%20%7C~%20%222%5B0-9%5D%24%22%20%5B1s%5D)&start=${start}000000&end=${end}000000&step=2`
    );
    adjustMatrixResult(resp);
    expect(resp.data).toMatchSnapshot();
    // high level
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=1788&query=sum%20by%20(test_id)%20(rate(%7Btest_id%3D%22${testID}%22%7D%20%7C~%20%222%5B0-9%5D%24%22%20%5B1s%5D))&start=${start}000000&end=${end}000000&step=2`
    );
    adjustMatrixResult(resp);
    expect(resp.data).toMatchSnapshot();
    //aggregation empty
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=1788&query=rate(%7Btest_id%3D%22${testID}%22%2C%20freq%3D%222%22%7D%20%7C~%20%222%5B0-9%5D%24%22%20%5B1s%5D)&start=${start - 3600 * 1000}000000&end=${end - 3600 * 1000}000000&step=2`
    );
    adjustMatrixResult(resp);
    expect(resp.data).toMatchSnapshot();
    // high level empty
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=1788&query=sum%20by%20(test_id)%20(rate(%7Btest_id%3D%22${testID}%22%7D%20%7C~%20%222%5B0-9%5D%24%22%20%5B1s%5D))&start=${start - 3600 * 1000}000000&end=${end - 3600 * 1000}000000&step=2`
    );
    adjustMatrixResult(resp);
    expect(resp.data).toMatchSnapshot();
});