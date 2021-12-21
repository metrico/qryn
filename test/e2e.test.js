const { createPoints, sendPoints } = require('./common')
const axios = require('axios')
const { WebSocket } = require('ws')
// const pb = require("protobufjs");
const e2e = () => process.env.INTEGRATION_E2E || process.env.INTEGRATION
const clokiLocal = () => process.env.CLOKI_LOCAL || process.env.CLOKI_EXT_URL || false
let l = null

// const root = pb.loadSync(__dirname + "/../lib/loki.proto");
// const pushMessage = root.lookupType("logproto.PushRequest");

beforeAll(() => {
  // await new Promise(f => setTimeout(f, 500));
  jest.setTimeout(300000)
  return setup()
})

function setup () {
  if (!e2e()) {
    return
  }
  if (!clokiLocal()) l = require('../cloki')
  return new Promise(resolve => setTimeout(resolve, 1000))
}
afterAll(() => {
  if (!e2e()) {
    return
  }
  if (!clokiLocal()) l.stop()
})

/* async function pushPBPoints(endpoint, points) {
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
} */

jest.setTimeout(300000)

it('e2e', async () => {
  if (!e2e()) {
    return
  }
  console.log('Waiting 2s before all inits')
  const clokiExtUrl = process.env.CLOKI_EXT_URL || 'localhost:3100'
  await new Promise(resolve => setTimeout(resolve, 2000))
  const testID = Math.random() + ''
  console.log(testID)
  const start = Math.floor((Date.now() - 60 * 1000 * 10) / 60 / 1000) * 60 * 1000
  const end = Math.floor(Date.now() / 60 / 1000) * 60 * 1000
  let points = createPoints(testID, 0.5, start, end, {}, {})
  points = createPoints(testID, 1, start, end, {}, points)
  points = createPoints(testID, 2, start, end, {}, points)
  points = createPoints(testID, 4, start, end, {}, points)

  points = createPoints(testID + '_json', 1, start, end,
    { fmt: 'json', lbl_repl: 'val_repl', int_lbl: '1' }, points,
    (i) => JSON.stringify({ lbl_repl: 'REPL', int_val: '1', new_lbl: 'new_val', str_id: i, arr: [1, 2, 3], obj: { o_1: 'v_1' } })
  )
  points = createPoints(testID + '_metrics', 1, start, end,
    { fmt: 'int', lbl_repl: 'val_repl', int_lbl: '1' }, points,
    (i) => '',
    (i) => i % 10
  )
  points = createPoints(testID + '_logfmt', 1, start, end,
    { fmt: 'logfmt', lbl_repl: 'val_repl', int_lbl: '1' }, points,
    (i) => 'lbl_repl="REPL" int_val=1 new_lbl="new_val" str_id="' + i + '" '
  )
  await sendPoints(`http://${clokiExtUrl}`, points)
  await new Promise(resolve => setTimeout(resolve, 4000))
  const adjustResult = (resp, id, _start) => {
    _start = _start || start
    id = id || testID
    resp.data.data.result = resp.data.data.result.map(stream => {
      expect(stream.stream.test_id).toEqual(id)
      stream.stream.test_id = 'TEST_ID'
      stream.values = stream.values.map(v => [v[0] - _start * 1000000, v[1]])
      return stream
    })
  }
  const runRequest = (req, _step, _start, _end) => {
    _start = _start || start
    _end = _end || end
    _step = _step || 2
    return axios.get(
            `http://${clokiExtUrl}/loki/api/v1/query_range?direction=BACKWARD&limit=2000&query=${encodeURIComponent(req)}&start=${_start}000000&end=${_end}000000&step=${_step}`
    )
  }
  const adjustMatrixResult = (resp, id) => {
    id = id || testID
    resp.data.data.result = resp.data.data.result.map(stream => {
      expect(stream.metric.test_id).toEqual(id)
      stream.metric.test_id = 'TEST_ID'
      stream.values = stream.values.map(v => [v[0] - Math.floor(start / 1000), v[1]])
      return stream
    })
  }

  // ok limited res
  let resp = await runRequest(`{test_id="${testID}"}`)
  console.log('TEST ID=' + testID)
  adjustResult(resp)
  expect(resp.data).toMatchSnapshot()
  // empty res
  resp = await runRequest(`{test_id="${testID}"}`, 2, start - 3600 * 1000, end - 3600 * 1000)
  adjustResult(resp)
  expect(resp.data).toMatchSnapshot()
  // two clauses
  resp = await runRequest(`{test_id="${testID}", freq="2"}`)
  adjustResult(resp)
  expect(resp.data).toMatchSnapshot()
  // two clauses and filter
  resp = await runRequest(`{test_id="${testID}", freq="2"} |~ "2[0-9]$"`)
  adjustResult(resp)
  expect(resp.data).toMatchSnapshot()
  // aggregation
  resp = await runRequest(`rate({test_id="${testID}", freq="2"} |~ "2[0-9]$" [1s])`)
  adjustMatrixResult(resp)
  expect(resp.data).toMatchSnapshot()
  // hammering aggregation
  for (const fn of ['count_over_time', 'bytes_rate', 'bytes_over_time', 'absent_over_time']) {
    resp = await runRequest(`${fn}({test_id="${testID}", freq="2"} |~ "2[0-9]$" [1s])`)
    expect(resp.data.data.result.length).toBeTruthy()
  }
  // high level
  resp = await runRequest(`sum by (test_id) (rate({test_id="${testID}"} |~ "2[0-9]$" [1s]))`)
  adjustMatrixResult(resp)
  expect(resp.data).toMatchSnapshot()
  // hammering high level
  for (const fn of ['min', 'max', 'avg', 'stddev', 'stdvar', 'count']) {
    resp = await runRequest(`${fn} by (test_id) (rate({test_id="${testID}"} |~ "2[0-9]$" [1s]))`)
    expect(resp.data.data.result.length).toBeTruthy()
  }
  // aggregation empty
  resp = await runRequest(`rate({test_id="${testID}", freq="2"} |~ "2[0-9]$" [1s])`,
    2, start - 3600 * 1000, end - 3600 * 1000)
  adjustMatrixResult(resp)
  expect(resp.data).toMatchSnapshot()
  // high level empty
  resp = await runRequest(`sum by (test_id) (rate({test_id="${testID}"} |~ "2[0-9]$" [1s]))`,
    2, start - 3600 * 1000, end - 3600 * 1000)
  adjustMatrixResult(resp)
  expect(resp.data).toMatchSnapshot()
  // json without params
  resp = await runRequest(`{test_id="${testID}_json"}|json`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  // json with params
  resp = await runRequest(`{test_id="${testID}_json"}|json lbl_repl="new_lbl"`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  // json with params / stream_selector
  resp = await runRequest(`{test_id="${testID}_json"}|json lbl_repl="new_lbl"|lbl_repl="new_val"`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  // json with params / stream_selector
  resp = await runRequest(`{test_id="${testID}_json"}|json lbl_repl="new_lbl"|fmt="json"`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  // json with no params / stream_selector
  resp = await runRequest(`{test_id="${testID}_json"}|json|fmt=~"[jk]son"`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  // json no params / stream_selector
  resp = await runRequest(`{test_id="${testID}_json"}|json|lbl_repl="REPL"`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  // unwrap
  resp = await runRequest(`sum_over_time({test_id="${testID}_json"}|json` +
    '|lbl_repl="REPL"|unwrap int_lbl [3s]) by (test_id, lbl_repl)')
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  // hammering unwrap
  for (const fn of ['rate', 'sum_over_time', 'avg_over_time', 'max_over_time', 'min_over_time',
    'first_over_time', 'last_over_time'
    // , 'stdvar_over_time', 'stddev_over_time', 'quantile_over_time', 'absent_over_time'
  ]) {
    resp = await runRequest(`${fn}({test_id="${testID}_json"}|json` +
      '|lbl_repl="REPL"|unwrap int_lbl [3s]) by (test_id, lbl_repl)')
    try {
      expect(resp.data.data.result.length).toBeTruthy()
    } catch (e) {
      console.log(fn)
      throw e
    }
  }
  resp = await runRequest(`sum_over_time({test_id="${testID}_json"}|json lbl_int1="int_val"` +
    '|lbl_repl="val_repl"|unwrap lbl_int1 [3s]) by (test_id, lbl_repl)')
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}"}| line_format ` +
    '"{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"')
  adjustResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`rate({test_id="${testID}"}` +
    '| line_format "{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"' +
    '| json|unwrap freq2 [1s]) by (test_id, freq2)')
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`rate({test_id="${testID}"}` +
    '| line_format "{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"' +
    '| json|unwrap freq2 [1s]) by (test_id, freq2)', 60)
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}_json"}|json|json int_lbl2="int_val"`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}_json"}| line_format "{{ divide test_id 2  }}"`)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`rate({test_id="${testID}_json"}| line_format "{{ divide int_lbl 2  }}" | unwrap _entry [1s])`)
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum(rate({test_id="${testID}_json"}| json [5s])) by (test_id)`)
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum(rate({test_id="${testID}_json"}| json lbl_rrr="lbl_repl" [5s])) by (test_id, lbl_rrr)`)
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum(sum_over_time({test_id="${testID}_json"}| json | unwrap int_val [10s]) by (test_id, str_id)) by (test_id)`)
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`derivative({test_id="${testID}_json"}| json | unwrap str_id [10s]) by (test_id)`)
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`rate({test_id="${testID}"} [1s]) == 2`)
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum(rate({test_id="${testID}"} [1s])) by (test_id) > 4`)
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum(sum_over_time({test_id="${testID}_json"}| json | unwrap str_id [10s]) by (test_id, str_id)) by (test_id) > 1000`)
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`rate({test_id="${testID}"} | line_format "12345" [1s]) == 2`)
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`derivative({test_id="${testID}_json"}| json | unwrap str_id [10s]) by (test_id) > 1`)
  adjustMatrixResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}"} | freq >= 4`)
  adjustResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}_json"} | json sid="str_id" | sid >= 598`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}_json"} | json | str_id >= 598`)
  adjustResult(resp, testID + '_json')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`test_macro("${testID}")`)
  adjustResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}"} | regexp "^(?<e>[^0-9]+)[0-9]+$"`)
  adjustResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}"} | regexp "^[^0-9]+(?<e>[0-9])+$"`)
  adjustResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}"} | regexp "^[^0-9]+([0-9]+(?<e>[0-9]))$"`)
  adjustResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`first_over_time({test_id="${testID}", freq="0.5"} | regexp "^[^0-9]+(?<e>[0-9]+)$" | unwrap e [1s]) by(test_id)`, 1)
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()

  const ws = new WebSocket(`ws://${clokiExtUrl}/loki/api/v1/tail?query={test_id="${testID}_ws"}`)
  resp = {
    data: {
      data: {
        result: []
      }
    }
  }
  ws.on('message', (msg) => {
    const _msg = JSON.parse(msg)
    for (const stream of _msg.streams) {
      let _stream = resp.data.data.result.find(res =>
        JSON.stringify(res.stream) === JSON.stringify(stream.stream)
      )
      if (!_stream) {
        _stream = {
          stream: stream.stream,
          values: []
        }
        resp.data.data.result.push(_stream)
      }
      _stream.values.push(...stream.values)
    }
  })
  const wsStart = Math.floor(Date.now() / 1000) * 1000
  for (let i = 0; i < 5; i++) {
    const points = createPoints(testID + '_ws', 1, wsStart + i * 1000, wsStart + i * 1000 + 1000, {}, {},
      () => `MSG_${i}`)
    sendPoints(`http://${clokiExtUrl}`, points)
    await new Promise(resolve => setTimeout(resolve, 1000))
  }
  await new Promise(resolve => setTimeout(resolve, 6000))
  ws.close()
  for (const res of resp.data.data.result) {
    res.values.sort()
  }
  adjustResult(resp, testID + '_ws', wsStart)
  expect(resp.data).toMatchSnapshot()
  resp = await axios.get(`http://${clokiExtUrl}/loki/api/v1/series?match={test_id="${testID}"}&start=1636008723293000000&end=1636012323293000000`)
  resp.data.data = resp.data.data.map(l => {
    expect(l.test_id).toEqual(testID)
    return { ...l, test_id: 'TEST' }
  })
  resp.data.data.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))
  expect(resp.data).toMatchSnapshot()
  resp = await axios.get(`http://${clokiExtUrl}/loki/api/v1/series?match={test_id="${testID}"}&match={test_id="${testID}_json"}&start=1636008723293000000&end=1636012323293000000`)
  resp.data.data = resp.data.data.map(l => {
    expect(l.test_id.startsWith(testID))
    return { ...l, test_id: l.test_id.replace(testID, 'TEST') }
  })
  resp.data.data.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))
  expect(resp.data).toMatchSnapshot()
  await new Promise(resolve => setTimeout(resolve, 1000))
  resp = await runRequest(`{test_id="${testID}"} | freq > 1 and (freq="4" or freq==2 or freq > 0.5)`)
  adjustResult(resp, testID)
  expect(resp.data.data.result.map(s => [s.stream, s.values.length])).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}_json"} | json sid="str_id" | sid >= 598 or sid < 2 and sid > 0`)
  adjustResult(resp, testID + '_json')
  expect(resp.data.data.result.map(s => [s.stream, s.values.length])).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}_json"} | json | str_id < 2 or str_id >= 598 and str_id > 0`)
  adjustResult(resp, testID + '_json')
  expect(resp.data.data.result.map(s => [s.stream, s.values.length])).toMatchSnapshot()
  resp = await runRequest(`sum_over_time({test_id="${testID}_json"}` +
    '| json| label_to_row "str_id, int_lbl"| unwrap _entry [10s])')
  resp.data.data.result = resp.data.data.result.map(stream => {
    stream.values = stream.values.map(v => [v[0] - Math.floor(start / 1000), v[1]])
    return stream
  })
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum_over_time({test_id="${testID}_metrics"} | unwrap_value [10s])`)
  adjustMatrixResult(resp, `${testID}_metrics`)
  expect(resp.data).toMatchSnapshot()
  // console.log(JSON.stringify(resp.data.data.result.map(s => [s.stream, s.values.length])))
  // logfmt without params
  resp = await runRequest(`{test_id="${testID}_logfmt"}|logfmt`)
  adjustResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  // logfmt with no params / stream_selector
  resp = await runRequest(`{test_id="${testID}_logfmt"}|logfmt|fmt=~"[jk]son"`)
  adjustResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  // logfmt no params / stream_selector
  resp = await runRequest(`{test_id="${testID}_logmft"}|logfmt|lbl_repl="REPL"`)
  adjustResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum_over_time({test_id="${testID}_logfmt"}|logfmt` +
    '|lbl_repl="REPL"|unwrap int_lbl [3s]) by (test_id, lbl_repl)')
  adjustMatrixResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  // hammering aggregation
  for (const fn of ['rate', 'sum_over_time', 'avg_over_time', 'max_over_time', 'min_over_time',
    'first_over_time', 'last_over_time'
    // , 'stdvar_over_time', 'stddev_over_time', 'quantile_over_time', 'absent_over_time'
  ]) {
    resp = await runRequest(`${fn}({test_id="${testID}_logfmt"}|logfmt` +
      '|lbl_repl="REPL"|unwrap int_lbl [3s]) by (test_id, lbl_repl)')
    try {
      expect(resp.data.data.result.length).toBeTruthy()
    } catch (e) {
      console.log(fn)
      throw e
    }
  }
  resp = await runRequest(`rate({test_id="${testID}"}` +
    '| line_format "str=\\"{{_entry}}\\" freq2={{divide freq 2}}"' +
    '| logfmt | unwrap freq2 [1s]) by (test_id, freq2)')
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`rate({test_id="${testID}"}` +
    '| line_format "str=\\"{{_entry}}\\" freq2={{divide freq 2}}"' +
    '| logfmt | unwrap freq2 [1s]) by (test_id, freq2)', 60)
  adjustMatrixResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum(rate({test_id="${testID}_logfmt"}| logfmt [5s])) by (test_id)`)
  adjustMatrixResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`sum(sum_over_time({test_id="${testID}_logfmt"}| logfmt | unwrap int_val [10s]) by (test_id, str_id)) by (test_id)`)
  adjustMatrixResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`derivative({test_id="${testID}_logfmt"}| logfmt | unwrap str_id [10s]) by (test_id)`)
  adjustMatrixResult(resp, testID + '_logfmt')
  resp = await runRequest(`sum(sum_over_time({test_id="${testID}_logfmt"}| logfmt | unwrap str_id [10s]) by (test_id, str_id)) by (test_id) > 1000`)
  adjustMatrixResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`derivative({test_id="${testID}_logfmt"}| logfmt | unwrap str_id [10s]) by (test_id) > 1`)
  adjustMatrixResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`{test_id="${testID}_logfmt"} | logfmt | str_id >= 598`)
  adjustResult(resp, testID + '_logfmt')
  expect(resp.data).toMatchSnapshot()
  resp = await runRequest(`rate({test_id="${testID}_json"} | json int_val="int_val" | unwrap int_val [1m]) by (test_id)`,
    0.05)
  expect(resp.data.data.result.length > 0).toBeTruthy()
  process.env.LINE_FMT = 'go_native'
  resp = await runRequest(`{test_id="${testID}"}| line_format ` +
    '"{ \\"str\\":\\"{{ ._entry }}\\", \\"freq2\\": {{ .freq }} }"')
  adjustResult(resp, testID)
  expect(resp.data).toMatchSnapshot()
  process.env.LINE_FMT = 'handlebars'
})
