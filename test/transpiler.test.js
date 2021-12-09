const bnf = require('../parser/bnf')
const transpiler = require('../parser/transpiler')
const { DataStream } = require('scramjet')
const { DATABASE_NAME, samplesReadTableName } = require('../lib/utils')
const { sharedParamNames } = require('../parser/registry/common')

beforeAll(() => {
  process.env.CLICKHOUSE_DB = 'loki'
})

const setQueryParam = (query, name, val) => {
  if (query.getParam(name)) {
    query.getParam(name).set(val)
  }
}

const setParams = (query) => {
  setQueryParam(query, sharedParamNames.timeSeriesTable, `${DATABASE_NAME()}.time_series`)
  setQueryParam(query, sharedParamNames.samplesTable, `${DATABASE_NAME()}.${samplesReadTableName}`)
  setQueryParam(query, sharedParamNames.from, 1)
  setQueryParam(query, sharedParamNames.to, 2)
  setQueryParam(query, sharedParamNames.limit, 3)
}

it('should transpile log_stream_selector', () => {
  let scr = '{et_dolorem=`nemo doloremque`, quia="eum voluptatem non eligendi"}'
  let script = bnf.ParseScript(scr)
  let query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = '{rerum_laborum=~`^con.+q.at[a-z]r`}'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = '{et_dolorem!=`nemo doloremque`}'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = '{rerum_laborum!~`^con.+q.at[a-z]r`}'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()
})

it('should transpile log_stream_selector with stream filter', () => {
  let scr = '{et_dolorem=`nemo doloremque`, quia="eum voluptatem non eligendi"} |= "at et"'
  let script = bnf.ParseScript(scr)
  let query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = '{rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta"'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = '{et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus"'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = '{rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta"'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()
})
describe('log_range_aggregation', () => {
  const test = (scr) => {
    const script = bnf.ParseScript(scr)
    const q = transpiler.initQuery()
    const query = transpiler.transpileLogRangeAggregation(script.rootToken, q)
    setParams(query)
    expect(query).toMatchSnapshot()
    expect(query.toString()).toMatchSnapshot()
  }
  it('1', () => {
    test('rate({minus_nam="aut illo"}[5m])')
  })
  it('2', () => test('rate({rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta" [5m])'))
  it('3', () => test('rate({et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus" [5m])'))
  it('4', () => test('rate({rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta" [5m])'))
})

it('should transpile aggregation_operator', () => {
  let scr = 'sum (rate({minus_nam="aut illo"}[5m])) by (label_1)'
  let script = bnf.ParseScript(scr)
  let q = transpiler.initQuery()
  q.ctx = {
    start: 0,
    end: 3600 * 1000
  }
  let query = transpiler.transpileAggregationOperator(script.rootToken, q)
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = 'sum by (label_1) (rate({rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta" [5m]))'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileAggregationOperator(script.rootToken, q)
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  scr = 'sum by (label_1)  (rate({minus_nam="aut illo"}[5m]))'
  script = bnf.ParseScript(scr)
  q = transpiler.initQuery()
  q.ctx = {
    start: 0,
    end: 3600 * 1000
  }
  query = transpiler.transpileAggregationOperator(script.rootToken, q)
  setParams(query)
  expect(query).toMatchSnapshot()
  expect(query.toString()).toMatchSnapshot()

  /* scr = 'rate({et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpileAggregationOperator(script.rootToken, transpiler.initQuery());
    expect(query).toMatchSnapshot();
    expect(query.toString()).toMatchSnapshot();

    scr = 'rate({rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpileAggregationOperator(script.rootToken, transpiler.initQuery());
    expect(query).toMatchSnapshot();
    expect(query.toString()).toMatchSnapshot(); */
})

it('should transpile json requests', async () => {
  let script = bnf.ParseScript('{autem_quis="quidem sit"}| json odit_iusto="dicta"')
  let res = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  setParams(res)
  expect(res).toMatchSnapshot()
  script = bnf.ParseScript('{autem_quis="quidem sit"}| json')
  res = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  let stream = DataStream.from([{
    labels: { autem_quis: 'quidem sit', l1: 'v1', l2: 'v2' },
    string: JSON.stringify({ l1: 'v3', l3: 'v4' })
  }])
  res.ctx.stream.forEach(f => { stream = f(stream) })
  res = await stream.toArray()
  expect(res).toMatchSnapshot()
})

it('should transpile logfmt requests', async () => {
  const script = bnf.ParseScript('{autem_quis="quidem sit"}| logfmt')
  let res = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  let stream = DataStream.from([{
    labels: { autem_quis: 'quidem sit', l1: 'v1', l2: 'v2' },
    string: 'l1="v3" l3="v4" '
  }])
  res.ctx.stream.forEach(f => { stream = f(stream) })
  res = await stream.toArray()
  expect(res).toMatchSnapshot()
})

it('shoud transpile unwrap', async () => {
  let q = transpiler.initQuery()
  q.ctx.step = 120000
  let script = bnf.ParseScript('rate({test_id="0.7857680014573265_json"}| unwrap int_lbl [1m]) by (fmt)')
  expect(script).toBeTruthy()
  q = transpiler.initQuery()
  q.ctx.step = 120000
  let req = transpiler.transpileUnwrapFunction(script.rootToken, q)
  expect(req).toMatchSnapshot()

  script = bnf.ParseScript('rate({test_id="0.7857680014573265_json"}| json int_lbl2="int_val"| unwrap int_lbl2 [1m]) by (fmt)')
  q = transpiler.initQuery()
  q.ctx.step = 120000
  req = transpiler.transpileUnwrapFunction(script.rootToken, q)
  expect(req).toMatchSnapshot()
  script = bnf.ParseScript('rate({test_id="0.7857680014573265_json"}| json int_lbl2="int_val"| unwrap int_lbl [1m]) by (int_lbl2)')
  q = transpiler.initQuery()
  q.ctx.step = 120000
  req = transpiler.transpileUnwrapFunction(script.rootToken, q)
  expect(req).toMatchSnapshot()

  const testData = [{
    timestamp_ms: 0,
    labels: { test_id: '0.7857680014573265_json', freq: '1', fmt: 'json', lbl_repl: 'val_repl', int_lbl: '1' },
    string: JSON.stringify({ lbl_repl: 'REPL', int_val: '1', new_lbl: 'new_val', str_id: 0, arr: [1, 2, 3], obj: { o_1: 'v_1' } })
  }, {
    timestamp_ms: 1000,
    labels: { test_id: '0.7857680014573265_json', freq: '1', fmt: 'json', lbl_repl: 'val_repl', int_lbl: '1' },
    string: JSON.stringify({ lbl_repl: 'REPL', int_val: '1', new_lbl: 'new_val', str_id: 0, arr: [1, 2, 3], obj: { o_1: 'v_1' } })
  }, {
    timestamp_ms: 2000,
    labels: { test_id: '0.7857680014573265_json', freq: '1', fmt: 'json', lbl_repl: 'val_repl', int_lbl: '1' },
    string: JSON.stringify({ lbl_repl: 'REPL', int_val: 'ewew', new_lbl: 'new_val', str_id: 0, arr: [1, 2, 3], obj: { o_1: 'v_1' } })
  }, { EOF: true }]
  script = bnf.ParseScript('sum_over_time({test_id="0.7857680014573265_json"}| json| unwrap int_val [2s]) by (freq)')
  q = transpiler.initQuery()
  q.ctx.step = 1000
  req = transpiler.transpileUnwrapFunction(script.rootToken, q)
  let ds = DataStream.fromArray(testData)
  req.ctx.stream.forEach(s => {
    ds = s(ds)
  })
  const res = await ds.toArray()
  expect(res).toEqual([{ labels: { freq: '1' }, timestamp_ms: '0', value: 2 }, { EOF: true }])

  expect(() => transpiler.transpile({ query: 'rate({test_id="1"} |~ "123" | unwrap_value [1s])' }))
    .toThrowError('log pipeline not supported')

  /* expect(res).toMatchSnapshot();
    script = bnf.ParseScript(`{test_id="0.7857680014573265_json"}| json| unwrap int_lbl`);
    req = transpiler.transpile_unwrap_expression(script.rootToken, transpiler.initQuery());
    ds = DataStream.fromArray(testData);
    req.stream.forEach(s => {
        ds = s(ds);
    });
    res = await ds.toArray();
    expect(res).toMatchSnapshot(); */
})

it('should transpile complex pipelines', async () => {
  const q = transpiler.transpile({
    query: '{test_id="${testID}"} | freq >= 4',
    limit: 1000,
    direction: 'forward',
    start: '1',
    end: '100000000000000',
    step: 1,
    stream: []
  })
  expect(q).toMatchSnapshot()
})

it('should transpile line format', async () => {
  let script = bnf.ParseScript('{a="b"} | line_format "{{_entry}} {{lbl1}} {{divide int 2}}"')
  let q = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  let ds = DataStream.fromArray([{ labels: { lbl1: 'a', int: 10 }, string: 'str' }])
  q.ctx.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()
  script = bnf.ParseScript('{a="b"} | line_format "{ \\"entry\\": \\"{{_entry}}\\", \\"intval\\": {{divide int 2}} }" | json')
  q = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  ds = DataStream.fromArray([{ labels: { lbl1: 'a', int: 10 }, string: 'str' }])
  q.ctx.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()

  q = transpiler.initQuery()
  q.ctx.step = 1000
  script = bnf.ParseScript('rate({a="b"} | line_format "{ \\"entry\\": \\"{{_entry}}\\", \\"intval\\": {{divide int 2}} }" | json | unwrap intval [1s])')
  q = transpiler.transpileUnwrapFunction(script.rootToken, q)
  ds = DataStream.fromArray([{ labels: { lbl1: 'a', int: 10 }, timestamp_ms: 0, string: 'str' }, { EOF: true }])
  q.ctx.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()
  // console.log(await ds.toArray());
})

it('should transpile plugins', async () => {
  const script = bnf.ParseScript('derivative({a="b"} | unwrap int [10s])')
  const _q = transpiler.initQuery()
  _q.ctx.step = 1000
  const q = transpiler.transpileUnwrapFunction(script.rootToken, _q)
  let ds = DataStream.fromArray([
    { labels: { lbl1: 'a' }, unwrapped: 10, timestamp_ms: 0, string: 'str' },
    { labels: { lbl1: 'a' }, unwrapped: 20, timestamp_ms: 1000, string: 'str' },
    { labels: { lbl1: 'a' }, unwrapped: 30, timestamp_ms: 2000, string: 'str' },
    { EOF: true }
  ])
  q.ctx.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()
})

it('should transpile macro', async () => {
  const script = bnf.ParseScript('test_macro("b")')
  expect(transpiler.transpileMacro(script.rootToken.Child('user_macro')))
    .toMatch('{test_id="b"}')
})

describe('should transpile new style', () => {
  const cq = (q) => ({
    direction: 'BACKWARD',
    limit: '2000',
    query: q,
    start: '1638802620000000000',
    end: '1638803220000000000',
    step: '2'
  })
  it('1', () => {
    const res = transpiler.transpile(cq('{test_id=\"0.7387779420506657\"}'))
    expect(res).toMatchSnapshot()
  })
  it('2', () => {
    const res = transpiler.transpile(cq('{test_id=\"0.2119268970232\", freq=\"2\"} |~ \"2[0-9]$\"'))
    expect(res).toMatchSnapshot()
  })
  it('3', () => {
    const res = transpiler.transpile(cq('rate({test_id=\"0.7026038163617259\", freq=\"2\"} |~ \"2[0-9]$\" [1s])'))
    expect(res).toMatchSnapshot()
  })
  it('4', () => {
    const res = transpiler.transpile(cq(
      'absent_over_time({test_id=\"0.7026038163617259\", freq=\"2\"} |~ \"2[0-9]$\" [1s])'
    ))
    expect(res).toMatchSnapshot()
  })
  it('5', () => {
    const res = transpiler.transpile(cq('{test_id="0.000341166036469831_json"}|json'))
    expect(res).toMatchSnapshot()
  })
  it('6', () => {
    const res = transpiler.transpile(cq(
      '{test_id=\"0.2053747382122484_json\"}|json lbl_repl=\"new_lbl\"|lbl_repl=\"new_val\"'
    ))
    expect(res).toMatchSnapshot()
  })
  it('7', () => {
    const res = transpiler.transpile(cq(
      'sum_over_time({test_id=\"0.1547558751138609_json\"}|json|lbl_repl=\"REPL\"|unwrap int_lbl [3s]) by (test_id, lbl_repl)'
    ))
    expect(res).toMatchSnapshot()
  })
  it('8', () => {
    const res = transpiler.transpile(cq(
      'rate({test_id=\"0.4075242197275857\"}| line_format \"{ \\\"str\\\":\\\"{{_entry}}\\\", \\\"freq2\\\": {{divide freq 2}} }\"| json|unwrap freq2 [1s]) by (test_id, freq2)'
    ))
    expect(res).toMatchSnapshot()
  })
  it('9', () => {
    const res = transpiler.transpile(cq(
      '{test_id=\"0.7186063017626447_json\"} | json sid=\"str_id\" | sid >= 598'
    ))
    expect(res).toMatchSnapshot()
  })
  it('10', () => {
    const res = transpiler.transpile(cq(
      '{test_id=\"0.5505504081219323\"} | regexp \"^(?<e>[^0-9]+)[0-9]+$\"'
    ))
    expect(res).toMatchSnapshot()
  })
})

it('should transpile tail', () => {
  const res = transpiler.transpileTail({ query: '{test_id=~"_ws"}' })
  expect(res).toMatchSnapshot()
})

it('should transpile series', () => {
  const res = transpiler.transpileSeries(['{test_id="123"}'])
  expect(res).toMatchSnapshot()
})
