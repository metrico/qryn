const {createPoints, sendPoints} = require("./common");
const axios = require("axios");
const pb = require("protobufjs");
const e2e = () => process.env.INTEGRATION_E2E || process.env.INTEGRATION;
const cloki_local = () => process.env.CLOKI_LOCAL || false;
let l = null;

const root = pb.loadSync(__dirname + "/../lib/loki.proto");
const pushMessage = root.lookupType("logproto.PushRequest");

beforeAll(() => {

    //await new Promise(f => setTimeout(f, 500));
    jest.setTimeout(300000);
    return setup()
});

function setup () {
  if (!e2e()) {
      return;
  }
  if (!cloki_local()) l = require("../cloki");
  return new Promise(f => setTimeout(f, 1000));
}
afterAll(() => {
    if (!e2e()) {
        return;
    }
    if (!cloki_local()) l.stop();
});

/*async function pushPBPoints(endpoint, points) {
    let req = Object.values(points).map((p) => {
        return {
            labels: "{" + Object.entries(p.stream).map((e) => `${e[0]}=${JSON.stringify(e[1])}`),
            entries: p.values.map(v => ({timestamp: {
                    seconds: Math.floor(parseInt(v[0]) / 1000000000),
                    nanos: parseInt(v[0]) % 1000000000
                }, line: v[1]}))
        }
    });
    req = pushMessage.fromObject(req);
}*/

jest.setTimeout(300000);

it("e2e", async () => {
    if (!e2e()) {
        return;
    }
    console.log("Waiting 2s before all inits");
    await new Promise(f => setTimeout(f, 2000));
    const testID = Math.random() + '';
    console.log(testID);
    const start = Math.floor((Date.now() - 60 * 1000 * 10) / 60 / 1000) * 60 * 1000;
    const end = Math.floor(Date.now() / 60 / 1000) * 60 * 1000;
    let points = createPoints(testID, 0.5, start, end, {}, {});
    points = createPoints(testID, 1, start, end, {}, points);
    points = createPoints(testID, 2, start, end, {}, points);
    points = createPoints(testID, 4, start, end, {}, points);

    points = createPoints(testID+"_json", 1, start, end,
        {fmt: "json", lbl_repl: "val_repl", int_lbl: "1"}, points,
        (i) => JSON.stringify({lbl_repl: 'REPL', int_val:'1', new_lbl: "new_val", str_id: i, arr: [1,2,3], obj: {o_1: "v_1"}})
        );
    await sendPoints('http://localhost:3100', points);
    await new Promise(f => setTimeout(f, 4000));
    // ok limited res
    let resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}"}&start=${start}000000&end=${end}000000&step=2`
    );
    const adjustResult = (resp, id) => {
        id = id || testID;
        resp.data.data.result = resp.data.data.result.map(stream => {
            expect(stream.stream.test_id).toEqual(id);
            stream.stream.test_id = "TEST_ID";
            stream.values = stream.values.map(v => [v[0] - start * 1000000, v[1]]);
            return stream;
        });
    }
    const runRequest = (req, _step, _start, _end) => {
        _start = _start || start;
        _end = _end || end;
        _step = _step || 2;
        return axios.get(
            `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query=${encodeURIComponent(req)}&start=${_start}000000&end=${_end}000000&step=${_step}`
        );
    }
    const adjustMatrixResult = (resp, id) => {
        id = id || testID;
        resp.data.data.result = resp.data.data.result.map(stream => {
            expect(stream.metric.test_id).toEqual(id);
            stream.metric.test_id = "TEST_ID";
            stream.values = stream.values.map(v => [v[0] - Math.floor(start / 1000), v[1]]);
            return stream;
        });
    }
    console.log('TEST ID=' + testID);
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
    // json without params
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}_json"}|json&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    // json with params
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}_json"}|json lbl_repl="new_lbl"&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    // json with params / stream_selector
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}_json"}|json lbl_repl="new_lbl"|lbl_repl="new_val"&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    // json with params / stream_selector
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}_json"}|json lbl_repl="new_lbl"|fmt="json"&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    // json with no params / stream_selector
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}_json"}|json|fmt=~"[jk]son"&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    // json no params / stream_selector
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}_json"}|json|lbl_repl="REPL"&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query=sum_over_time({test_id="${testID}_json"}|json|lbl_repl="REPL"|unwrap int_lbl [3s]) by (test_id, lbl_repl)&start=${start}000000&end=${end}000000&step=2`
    );
    adjustMatrixResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query=sum_over_time({test_id="${testID}_json"}|json lbl_int1="int_val"|lbl_repl="val_repl"|unwrap lbl_int1 [3s]) by (test_id, lbl_repl)&start=${start}000000&end=${end}000000&step=2`
    );
    adjustMatrixResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();

    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query={test_id="${testID}"}| line_format "{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"&start=${start}000000&end=${end}000000&step=2`
    );
    adjustResult(resp, testID);
    expect(resp.data).toMatchSnapshot();
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query=rate({test_id="${testID}"}| line_format "{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"|json|unwrap freq2 [1s]) by (test_id, freq2)&start=${start}000000&end=${end}000000&step=2`
    );

    adjustMatrixResult(resp, testID);
    expect(resp.data).toMatchSnapshot();
    resp = await axios.get(
        `http://localhost:3100/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query=rate({test_id="${testID}"}| line_format "{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"|json|unwrap freq2 [1s]) by (test_id, freq2)&start=${start}000000&end=${end}000000&step=60`
    );
    adjustMatrixResult(resp, testID);
    expect(resp.data).toMatchSnapshot();
    resp = await runRequest(`{test_id="${testID}_json"}|json|json int_lbl2="int_val"`);
    adjustResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    resp = await runRequest(`{test_id="${testID}_json"}| line_format "{{ divide test_id 2  }}"`);
    expect(resp.data).toMatchSnapshot();
    resp = await runRequest(`rate({test_id="${testID}_json"}| line_format "{{ divide int_lbl 2  }}" | unwrap _entry [1s])`);
    adjustMatrixResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    resp = await runRequest(`sum(rate({test_id="${testID}_json"}| json [5s])) by (test_id)`);
    adjustMatrixResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    resp = await runRequest(`sum(rate({test_id="${testID}_json"}| json lbl_rrr="lbl_repl" [5s])) by (test_id, lbl_rrr)`);
    adjustMatrixResult(resp, testID + "_json");
    expect(resp.data).toMatchSnapshot();
    //console.log(JSON.stringify(resp.data, 1));
});
