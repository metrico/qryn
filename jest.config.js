const path = require('path')
module.exports = {
  setupFilesAfterEnv: [path.join(__dirname, '/test/jest.setup.js')],
  moduleNameMapper: {
    '^axios$': 'axios/dist/node/axios.cjs'
  }
}
