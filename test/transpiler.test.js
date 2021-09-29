const bnf = require('../parser/bnf');
const transpiler = require('../parser/transpiler');
const {DataStream} = require('scramjet');

beforeAll(() => {
    process.env.CLICKHOUSE_DB = 'loki';
});

it('should transpile log_stream_selector', () => {
    let scr = '{et_dolorem=`nemo doloremque`, quia=\"eum voluptatem non eligendi\"}';
    let script = bnf.ParseScript(scr);
    let query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot()

    scr = '{rerum_laborum=~`^con.+q.at[a-z]r`}';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = '{et_dolorem!=`nemo doloremque`}';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = '{rerum_laborum!~`^con.+q.at[a-z]r`}';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();
});

it('should transpile log_stream_selector with stream filter', () => {
    let scr = '{et_dolorem=`nemo doloremque`, quia=\"eum voluptatem non eligendi\"} |= "at et"';
    let script = bnf.ParseScript(scr);
    let query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot()

    scr = '{rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta"';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = '{et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus"';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = '{rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta"';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();
});

it('should transpile log_range_aggregation', () => {
    let scr = 'rate({minus_nam="aut illo"}[5m])';
    let script = bnf.ParseScript(scr);
    let q = transpiler.init_query();
    q.ctx = {
        start: 0,
        end: 3600 * 1000
    };
    let query = transpiler.transpile_log_range_aggregation(script.rootToken, q);
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot()

     scr = 'rate({rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = 'rate({et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = 'rate({rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();
});


it('should transpile aggregation_operator', () => {
    let scr = 'sum (rate({minus_nam="aut illo"}[5m])) by (label_1)';
    let script = bnf.ParseScript(scr);
    let q = transpiler.init_query();
    q.ctx = {
        start: 0,
        end: 3600 * 1000
    };
    let query = transpiler.transpile_aggregation_operator(script.rootToken, q);
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot()

    scr = 'sum by (label_1) (rate({rerum_laborum=~`^con.+q.at[a-z]r`} != "consequatur nam soluta" [5m]))';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_aggregation_operator(script.rootToken, q);
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = 'sum by (label_1)  (rate({minus_nam="aut illo"}[5m]))';
    script = bnf.ParseScript(scr);
    q = transpiler.init_query();
    q.ctx = {
        start: 0,
        end: 3600 * 1000
    };
    query = transpiler.transpile_aggregation_operator(script.rootToken, q);
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot()

    /*scr = 'rate({et_dolorem!=`nemo doloremque`} |~ "^mol[eE][^ ]+e +voluptatibus" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_aggregation_operator(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();

    scr = 'rate({rerum_laborum!~`^con.+q.at[a-z]r`} !~ "cons[eE][^ ]+r nam soluta" [5m])';
    script = bnf.ParseScript(scr);
    query = transpiler.transpile_aggregation_operator(script.rootToken, transpiler.init_query());
    expect(query).toMatchSnapshot();
    expect(transpiler.request_to_str(query)).toMatchSnapshot();*/
});

it("should transpile json requests", async () => {
    let script = bnf.ParseScript(`{autem_quis="quidem sit"}| json odit_iusto="dicta"`);
    let res = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(res).toMatchSnapshot();
    script = bnf.ParseScript(`{autem_quis="quidem sit"}| json`);
    res = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    let stream = DataStream.from([{
        labels: {autem_quis: 'quidem sit', l1: 'v1', l2: 'v2'},
        string: JSON.stringify({l1: 'v3', l3: 'v4'})
    }]);
    res.stream.forEach(f => {stream = f(stream);});
    res = await stream.toArray();
    expect(res).toMatchSnapshot();
});

it ("shoud transpile unwrap", async () => {
    let q = {
        ...transpiler.init_query(),
        ctx: {step: 120000}
    };
    let script = bnf.ParseScript(`rate({test_id="0.7857680014573265_json"}| unwrap int_lbl [1m]) by (fmt)`);
    expect(script).toBeTruthy();
    let req = transpiler.transpile_unwrap_function(script.rootToken, q);
    expect(req).toMatchSnapshot();

    script = bnf.ParseScript(`rate({test_id="0.7857680014573265_json"}| json int_lbl2="int_val"| unwrap int_lbl2 [1m]) by (fmt)`);
    req = transpiler.transpile_unwrap_function(script.rootToken, q);
    expect(req).toMatchSnapshot();
    script = bnf.ParseScript(`rate({test_id="0.7857680014573265_json"}| json int_lbl2="int_val"| unwrap int_lbl [1m]) by (int_lbl2)`);
    req = transpiler.transpile_unwrap_function(script.rootToken, q);
    expect(req).toMatchSnapshot();

    const test_data = [{
        timestamp_ms: 0,
        labels: {"test_id":"0.7857680014573265_json","freq":"1","fmt":"json","lbl_repl":"val_repl","int_lbl":"1"},
        string: JSON.stringify({"lbl_repl":"REPL","int_val":"1","new_lbl":"new_val","str_id":0,"arr":[1,2,3],"obj":{"o_1":"v_1"}})
    },{
        timestamp_ms: 1000,
        labels: {"test_id":"0.7857680014573265_json","freq":"1","fmt":"json","lbl_repl":"val_repl","int_lbl":"1"},
        string: JSON.stringify({"lbl_repl":"REPL","int_val":"1","new_lbl":"new_val","str_id":0,"arr":[1,2,3],"obj":{"o_1":"v_1"}})
    },{
        timestamp_ms: 2000,
        labels: {"test_id":"0.7857680014573265_json","freq":"1","fmt":"json","lbl_repl":"val_repl","int_lbl":"1"},
        string: JSON.stringify({"lbl_repl":"REPL","int_val":"ewew","new_lbl":"new_val","str_id":0,"arr":[1,2,3],"obj":{"o_1":"v_1"}})
    }, { EOF: true }];
    script = bnf.ParseScript(`sum_over_time({test_id="0.7857680014573265_json"}| json| unwrap int_val [2s]) by (freq)`);
    q.ctx.step = 1000;
    req = transpiler.transpile_unwrap_function(script.rootToken, q);
    let ds = DataStream.fromArray(test_data);
    req.stream.forEach(s => {
        ds = s(ds);
    });
    let res = await ds.toArray();
    console.log(JSON.stringify(res, 1));

    /*expect(res).toMatchSnapshot();
    script = bnf.ParseScript(`{test_id="0.7857680014573265_json"}| json| unwrap int_lbl`);
    req = transpiler.transpile_unwrap_expression(script.rootToken, transpiler.init_query());
    ds = DataStream.fromArray(test_data);
    req.stream.forEach(s => {
        ds = s(ds);
    });
    res = await ds.toArray();
    expect(res).toMatchSnapshot();*/
});


it("should transpile line format", async () => {
    let script = bnf.ParseScript('{a="b"} | line_format "{{_entry}} {{lbl1}} {{divide int 2}}"')
    let q = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    let ds = DataStream.fromArray([{labels: {lbl1: 'a', int: 10}, string: 'str'}]);
    q.stream.forEach(s => { ds = s(ds)});
    expect(await ds.toArray()).toMatchSnapshot();
    script = bnf.ParseScript('{a="b"} | line_format "{ \\"entry\\": \\"{{_entry}}\\", \\"intval\\": {{divide int 2}} }" | json')
    q = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    ds = DataStream.fromArray([{labels: {lbl1: 'a', int: 10}, string: 'str'}]);
    q.stream.forEach(s => { ds = s(ds)});
    expect(await ds.toArray()).toMatchSnapshot();

    q = {
        ...transpiler.init_query(),
        ctx: {step: 1000}
    };
    script = bnf.ParseScript('rate({a="b"} | line_format "{ \\"entry\\": \\"{{_entry}}\\", \\"intval\\": {{divide int 2}} }" | json | unwrap intval [1s])')
    q = transpiler.transpile_unwrap_function(script.rootToken, q);
    ds = DataStream.fromArray([{labels: {lbl1: 'a', int: 10}, timestamp_ms: 0, string: 'str'}, { EOF: true }]);
    q.stream.forEach(s => {ds = s(ds)});
    expect(await ds.toArray()).toMatchSnapshot();
    //console.log(await ds.toArray());
});
