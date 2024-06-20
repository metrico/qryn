const parser = require('../traceql/parser')

it('traceql: one selector', () => {
  const res = parser.ParseScript('{.testId="12345"}')
  expect(res.rootToken.value).toEqual('{.testId="12345"}')
})

it('traceql: multiple selectors', () => {
  const res = parser.ParseScript('{.testId="12345" &&.spanN=9}')
  expect(res.rootToken.value).toEqual('{.testId="12345" &&.spanN=9}')
})

it('traceql: multiple selectors OR Brackets', () => {
  const res = parser.ParseScript('{.testId="12345" && (.spanN=9 ||.spanN=8)}')
  expect(res.rootToken.value).toEqual('{.testId="12345" && (.spanN=9 ||.spanN=8)}')
})

it('traceql: multiple selectors regexp', () => {
  const res = parser.ParseScript('{.testId="12345" &&.spanN=~"(9|8)"}')
  expect(res.rootToken.value).toEqual('{.testId="12345" &&.spanN=~"(9|8)"}')
})

it('traceql: duration', () => {
  const res = parser.ParseScript('{.testId="12345" && duration>=9ms}')
  expect(res.rootToken.value).toEqual('{.testId="12345" && duration>=9ms}')
})

it('traceql: float comparison', () => {
  const res = parser.ParseScript('{.testId="12345" &&.spanN>=8.9}')
  expect(res.rootToken.value).toEqual('{.testId="12345" &&.spanN>=8.9}')
})

it('traceql: count empty result', () => {
  const res = parser.ParseScript('{.testId="12345" &&.spanN>=8.9} | count() > 1')
  expect(res.rootToken.value).toEqual('{.testId="12345" &&.spanN>=8.9} | count() > 1')
})

it('traceql: max duration empty result', () => {
  const res = parser.ParseScript('{.testId="12345" &&.spanN>=8.9} | max(duration) > 9ms')
  expect(res.rootToken.value).toEqual('{.testId="12345" &&.spanN>=8.9} | max(duration) > 9ms')
})

it('traceql: max duration', () => {
  const res = parser.ParseScript('{.testId="12345" &&.spanN>=8.9} | max(duration) > 8ms')
  expect(res.rootToken.value).toEqual('{.testId="12345" &&.spanN>=8.9} | max(duration) > 8ms')
})

it('traceql: select', () => {
  const res = parser.ParseScript('{.testId="12345" &&.spanN>=8.9} | select(a, b)')
  expect(res.rootToken.value).toEqual('{.testId="12345" &&.spanN>=8.9} | select(a, b)')
})
