const bnf = require('../parser/bnf')
const regexp = require('../parser/registry/parser_registry/regexp')

it('should compile', () => {
  let res = bnf.ParseScript('bytes_rate({run="kokoko",u_ru_ru!="lolol",zozo=~"sssss"}  |~"atltlt" !~   "rmrmrm" [5m])')
  expect(res.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
    ['run="kokoko"', 'u_ru_ru!="lolol"', 'zozo=~"sssss"']
  )
  expect(res.rootToken.Children('log_pipeline').map(c => c.value)).toEqual(
    ['|~"atltlt"', '!~   "rmrmrm"']
  )
  res = bnf.ParseScript(
    'bytes_rate({run="kokoko",u_ru_ru!="lolol",zozo=~"sssss"}  |~"atltlt" !~   "rmrmrm" | line_format "{{run}} {{intval }}" [5m])'
  )
  expect(res).toBeTruthy()
  const tid = 0.1113693742057289
  res = bnf.ParseScript(`{test_id="${tid}"}| line_format "{ \\"str\\":\\"{{_entry}}\\", \\"freq2\\": {{divide freq 2}} }"`)
  expect(res).toBeTruthy()
})

it('should compile strings with escaped quotes', () => {
  const res = bnf.ParseScript('bytes_rate({run="kok\\"oko",u_ru_ru!="lolol",zozo=~"sssss"}  |~"atltlt" !~   "rmrmrm" [5m])')
  expect(res.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
    ['run="kok\\\"oko"', 'u_ru_ru!="lolol"', 'zozo=~"sssss"']
  )
  const res2 = bnf.ParseScript('bytes_rate({run=`kok\\`oko`,u_ru_ru!="lolol",zozo=~"sssss"}  |~"atltlt" !~   "rmrmrm" [5m])')
  expect(res2.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
    ['run=`kok\\`oko`', 'u_ru_ru!="lolol"', 'zozo=~"sssss"']
  )
  const res3 = bnf.ParseScript('bytes_rate({run=`kok\\\\\\`oko`,u_ru_ru!="lolol",zozo=~"sssss"}  |~"atltlt" !~   "rmrmrm" [5m])')
  expect(res3.rootToken.Children('log_stream_selector_rule').map(c => c.value)).toEqual(
    ['run=`kok\\\\\\`oko`', 'u_ru_ru!="lolol"', 'zozo=~"sssss"']
  )
})

it('should parse lbl cmp', () => {
  const ops = ['>', '<', '>=', '<=', '==', '!=']
  for (const op of ops) {
    const res = bnf.ParseScript(`{test_id="123345456"} | freq ${op} 4.0`)
    expect(res.rootToken.Child('number_label_filter_expression').value).toEqual(`freq ${op} 4.0`)
  }
  for (const op of ops) {
    const res = bnf.ParseScript(`{test_id="123345456"} | freq ${op} 4`)
    expect(res.rootToken.Child('number_label_filter_expression').value).toEqual(`freq ${op} 4`)
  }
})

it('should parse macros', () => {
  const res = bnf.ParseScript('test_macro("macro is ok")')
  expect(res.rootToken.value).toMatch('test_macro("macro is ok")')
  expect(res.rootToken.Child('quoted_str').value).toMatch('"macro is ok"')
})

const printTree = (token, indent, buf) => {
  buf = buf || ''
  if (token.name.match(/^(SCRIPT|SYNTAX|[a-z_]+)$/)) {
    buf += new Array(indent).fill(' ').join('') + token.name + ': ' + token.value + '\n'
  }
  buf = token.tokens.reduce((sum, t) => printTree(t, indent + 1, sum), buf)
  return buf
}

it('should compile regex', () => {
  expect(printTree(regexp.internal.compile('abcd\\('), 0)).toMatchSnapshot()
  expect(printTree(regexp.internal.compile('(a\\(bc)'), 0)).toMatchSnapshot()
  expect(printTree(regexp.internal.compile('(?<label1>a[^\\[\\(\\)]bc)'), 0)).toMatchSnapshot()
  expect(printTree(regexp.internal.compile('(a(?<label1>[^\\[\\(\\)]bc))'), 0)).toMatchSnapshot()
  expect(printTree(regexp.internal.compile('(a[\\(\\)]+(?<l2>b)(?<label1>[^\\[\\(\\)]bc))'), 0)).toMatchSnapshot()
})

it('should process regex', () => {
  expect(regexp.internal.extractRegexp('"(?<helper>[a-zA-Z0-9]+)..\\r\\n.(?<token>[a-zA-Z]+)"'))
    .toMatchSnapshot()
})

it('should get named groups', () => {
  const nGroups = (str) => {
    const t = regexp.internal.compile(str)
    const g = regexp.internal.walk(t, [])
    // console.log({n:str, g:g});
    expect(g).toMatchSnapshot()
  }
  nGroups('abcd\\(')
  nGroups('(a\\(bc)')
  nGroups('(?<label1>a[^\\[\\(\\)]bc)')
  nGroups('(a(?<label1>[^\\[\\(\\)]bc))')
  nGroups('(a[\\(\\)]+(?<l2>b)(?<label1>[^\\[\\(\\)]bc))')
})

it('should erase names', () => {
  const nGroups = (str) => {
    const t = regexp.internal.compile(str)
    const g = regexp.internal.rmNames(t)
    // console.log({n:str, g:g.value});
    expect(g.value).toMatchSnapshot()
  }
  nGroups('abcd\\(')
  nGroups('(a\\(bc)')
  nGroups('(?<label1>a[^\\[\\(\\)]bc)')
  nGroups('(a(?<label1>[^\\[\\(\\)]bc))')
  nGroups('(a[\\(\\)]+(?<l2>b)(?<label1>[^\\[\\(\\)]bc))')
})
