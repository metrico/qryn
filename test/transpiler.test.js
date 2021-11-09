const bnf = require('../parser/bnf')
const transpiler = require('../parser/transpiler')
const { DataStream } = require('scramjet')

beforeAll(() => {
  process.env.CLICKHOUSE_DB = 'loki'
})

it('should transpile log_stream_selector', () => {
  let scr = '{et_dolorem=`nemo doloremque`, quia=\"eum voluptatem non eligendi\"}'
  let script = bnf.ParseScript(scr)
  let query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = '{rerum_laborum=~`^con.+q.at[a-z]r`}'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = '{et_dolorem!=`nemo doloremque`}'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = '{rerum_laborum!~`^con.+q.at[a-z]r`}'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()
})

it('should transpile log_stream_selector with stream filter', () => {
  let scr = '{et_dolorem=`nemo doloremque`, quia=\"eum voluptatem non eligendi\"} |= "at et"'
  let script = bnf.ParseScript(scr)
  let query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = '{rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta"'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = '{et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus"'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = '{rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta"'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()
})

it('should transpile log_range_aggregation', () => {
  let scr = 'rate({minus_nam="aut illo"}[5m])'
  let script = bnf.ParseScript(scr)
  const q = transpiler.initQuery()
  q.ctx = {
    start: 0,
    end: 3600 * 1000
  }
  let query = transpiler.transpileLogRangeAggregation(script.rootToken, q)
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = 'rate({rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta" [5m])'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = 'rate({et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus" [5m])'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = 'rate({rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta" [5m])'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()
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
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = 'sum by (label_1) (rate({rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta" [5m]))'
  script = bnf.ParseScript(scr)
  query = transpiler.transpileAggregationOperator(script.rootToken, q)
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  scr = 'sum by (label_1)  (rate({minus_nam="aut illo"}[5m]))'
  script = bnf.ParseScript(scr)
  q = transpiler.initQuery()
  q.ctx = {
    start: 0,
    end: 3600 * 1000
  }
  query = transpiler.transpileAggregationOperator(script.rootToken, q)
  expect(query).toMatchSnapshot()
  expect(transpiler.requestToStr(query)).toMatchSnapshot()

  /* scr = 'rate({et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpileAggregationOperator(script.rootToken, transpiler.initQuery());
    expect(query).toMatchSnapshot();
    expect(transpiler.requestToStr(query)).toMatchSnapshot();

    scr = 'rate({rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpileAggregationOperator(script.rootToken, transpiler.initQuery());
    expect(query).toMatchSnapshot();
    expect(transpiler.requestToStr(query)).toMatchSnapshot(); */
})

it('should transpile json requests', async () => {
  let script = bnf.ParseScript('{autem_quis="quidem sit"}| json odit_iusto="dicta"')
  let res = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  expect(res).toMatchSnapshot()
  script = bnf.ParseScript('{autem_quis="quidem sit"}| json')
  res = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  let stream = DataStream.from([{
    labels: { autem_quis: 'quidem sit', l1: 'v1', l2: 'v2' },
    string: JSON.stringify({ l1: 'v3', l3: 'v4' })
  }])
  res.stream.forEach(f => { stream = f(stream) })
  res = await stream.toArray()
  expect(res).toMatchSnapshot()
})

it('shoud transpile unwrap', async () => {
  const q = {
    ...transpiler.initQuery(),
    ctx: { step: 120000 }
  }
  let script = bnf.ParseScript('rate({test_id="0.7857680014573265_json"}| unwrap int_lbl [1m]) by (fmt)')
  expect(script).toBeTruthy()
  let req = transpiler.transpileUnwrapFunction(script.rootToken, q)
  expect(req).toMatchSnapshot()

  script = bnf.ParseScript('rate({test_id="0.7857680014573265_json"}| json int_lbl2="int_val"| unwrap int_lbl2 [1m]) by (fmt)')
  req = transpiler.transpileUnwrapFunction(script.rootToken, q)
  expect(req).toMatchSnapshot()
  script = bnf.ParseScript('rate({test_id="0.7857680014573265_json"}| json int_lbl2="int_val"| unwrap int_lbl [1m]) by (int_lbl2)')
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
  q.ctx.step = 1000
  req = transpiler.transpileUnwrapFunction(script.rootToken, q)
  let ds = DataStream.fromArray(testData)
  req.stream.forEach(s => {
    ds = s(ds)
  })
  const res = await ds.toArray()
  expect(res).toEqual([{ labels: { freq: '1' }, timestamp_ms: '0', value: 2 }, { EOF: true }])

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

it('should transpile line format', async () => {
  let script = bnf.ParseScript('{a="b"} | line_format "{{_entry}} {{lbl1}} {{divide int 2}}"')
  let q = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  let ds = DataStream.fromArray([{ labels: { lbl1: 'a', int: 10 }, string: 'str' }])
  q.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()
  script = bnf.ParseScript('{a="b"} | line_format "{ \\"entry\\": \\"{{_entry}}\\", \\"intval\\": {{divide int 2}} }" | json')
  q = transpiler.transpileLogStreamSelector(script.rootToken, transpiler.initQuery())
  ds = DataStream.fromArray([{ labels: { lbl1: 'a', int: 10 }, string: 'str' }])
  q.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()

  q = {
    ...transpiler.initQuery(),
    ctx: { step: 1000 }
  }
  script = bnf.ParseScript('rate({a="b"} | line_format "{ \\"entry\\": \\"{{_entry}}\\", \\"intval\\": {{divide int 2}} }" | json | unwrap intval [1s])')
  q = transpiler.transpileUnwrapFunction(script.rootToken, q)
  ds = DataStream.fromArray([{ labels: { lbl1: 'a', int: 10 }, timestamp_ms: 0, string: 'str' }, { EOF: true }])
  q.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()
  // console.log(await ds.toArray());
})

it('should transpile plugins', async () => {
  const script = bnf.ParseScript('derivative({a="b"} | unwrap int [10s])')
  const _q = transpiler.initQuery()
  _q.ctx = { step: 1000 }
  const q = transpiler.transpileUnwrapFunction(script.rootToken, _q)
  let ds = DataStream.fromArray([
    { labels: { lbl1: 'a' }, unwrapped: 10, timestamp_ms: 0, string: 'str' },
    { labels: { lbl1: 'a' }, unwrapped: 20, timestamp_ms: 1000, string: 'str' },
    { labels: { lbl1: 'a' }, unwrapped: 30, timestamp_ms: 2000, string: 'str' },
    { EOF: true }
  ])
  q.stream.forEach(s => { ds = s(ds) })
  expect(await ds.toArray()).toMatchSnapshot()
})

it('should transpile macro', async () => {
  const script = bnf.ParseScript('test_macro("b")')
  expect(transpiler.transpileMacro(script.rootToken.Child('user_macro')))
    .toMatch('{test_id="b"}')
})
