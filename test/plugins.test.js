
const { getPlg } = require('../plugins/engine')
it('should glob', () => {
  expect(getPlg({ type: 'unwrap_registry' })).toBeTruthy()
})

it('should unicode chars', () => {
  console.log('АąŚĄ'.match(/\p{L}/ug))
})
