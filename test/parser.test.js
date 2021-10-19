const bnf = require('../parser/bnf');

it('should compile', () => {
    let res = bnf.ParseScript("bytes_rate({run=\"kokoko\",u_ru_ru!=\"lolol\",zozo=~\"sssss\"}  |~\"atltlt\" !~   \"rmrmrm\" [5m])");
    expect(res.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
        [ 'run="kokoko"', 'u_ru_ru!="lolol"', 'zozo=~"sssss"' ]
    );
    expect(res.rootToken.Children('log_pipeline').map(c => c.value)).toEqual(
        [ '|~"atltlt"', '!~   "rmrmrm"' ]
    );
    res = bnf.ParseScript(
        "bytes_rate({run=\"kokoko\",u_ru_ru!=\"lolol\",zozo=~\"sssss\"}  |~\"atltlt\" !~   \"rmrmrm\" | line_format \"{{run}} {{intval }}\" [5m])"
    )
    expect(res).toBeTruthy();
    const tid = 0.1113693742057289;
    res = bnf.ParseScript(`{test_id="${tid}"}| line_format "{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"`)
    expect(res).toBeTruthy();
});


it('should compile strings with escaped quotes', () => {
    const res = bnf.ParseScript("bytes_rate({run=\"kok\\\"oko\",u_ru_ru!=\"lolol\",zozo=~\"sssss\"}  |~\"atltlt\" !~   \"rmrmrm\" [5m])");
    expect(res.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
        [ 'run="kok\\\"oko"', 'u_ru_ru!="lolol"', 'zozo=~"sssss"' ]
    );
    const res2 = bnf.ParseScript("bytes_rate({run=`kok\\`oko`,u_ru_ru!=\"lolol\",zozo=~\"sssss\"}  |~\"atltlt\" !~   \"rmrmrm\" [5m])");
    expect(res2.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
        [ 'run=`kok\\`oko`', 'u_ru_ru!="lolol"', 'zozo=~"sssss"' ]
    );
    const res3 = bnf.ParseScript("bytes_rate({run=`kok\\\\\\`oko`,u_ru_ru!=\"lolol\",zozo=~\"sssss\"}  |~\"atltlt\" !~   \"rmrmrm\" [5m])");
    expect(res3.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
        [ 'run=`kok\\\\\\`oko`', 'u_ru_ru!="lolol"', 'zozo=~"sssss"' ]
    );
});

it('should glob', () => {
    const glob = require('glob');
    console.log(glob.sync('+(/home/hromozeka/QXIP/cLoki/plugins/unwrap_registry/**/plugnplay.yml|test_plugin/)'));
    console.log(glob.sync('/home/hromozeka/QXIP/cLoki/plugins/unwrap_registry/**/plugnplay.yml'));
    console.log(glob.sync('/home/hromozeka/QXIP/test_plugin/**/plugnplay.yml'));
});