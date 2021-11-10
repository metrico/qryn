
const { getPlg } = require('../plugins/engine')
it('should glob', () => {
  expect(getPlg({ type: 'unwrap_registry' })).toBeTruthy()
})
