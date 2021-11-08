
const { get_plg } = require('../plugins/engine')
it('should glob', () => {
  expect(get_plg({ type: 'unwrap_registry' })).toBeTruthy()
})
