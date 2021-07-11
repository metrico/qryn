const bnf = require('../parser/bnf');
const transpiler = require('../parser/transpiler');

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