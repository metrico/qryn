const bnf = require('../parser/bnf');
const transpiler = require('../parser/transpiler');

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

it("should transpile json requests", () => {
    const script = bnf.ParseScript(`{autem_quis="quidem sit"}| json odit_iusto="dicta"`);
    const res = transpiler.transpile_log_stream_selector(script.rootToken, transpiler.init_query());
    expect(res).toMatchSnapshot();
});

