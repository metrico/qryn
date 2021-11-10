const fs = require('fs')
const { createPoints, sendPoints } = require('./common')

/**
 * This is the Insert benchmark test.
 * In order to run the test you have to
 * - run clickhouse with appropriate databases
 * - provide all the needed environment for cLoki
 * - export LOKI_ENDPOINT=http://....loki endpoint...
 * - export SAME_DATA_BENCHMARK=1 env vars
 * - run jest
 */

const sameData = () => process.env.SAME_DATA_BENCHMARK === '1'

let l = null

beforeAll(async () => {
  if (!sameData()) {
    return
  }
  l = require('../cloki')
  await new Promise((resolve) => setTimeout(resolve, 500))
})

afterAll(() => {
  sameData() && l.stop()
})

it('should stream the same data to loki / cloki', async () => {
  if (!sameData()) {
    return
  }
  const testId = Date.now().toString()
  console.log(testId)
  const start = Date.now() - 60 * 1000
  const end = Date.now()
  let points = createPoints(testId, 1, start, end, {}, {})
  points = createPoints(testId, 2, start, end, {}, points)
  points = createPoints(testId, 4, start, end, {}, points)
  fs.writeFileSync('points.json', JSON.stringify({ streams: Object.values(points) }))
  await sendPoints('http://localhost:3100', points)
  await sendPoints(process.env.LOKI_ENDPOINT, points)
  await new Promise((resolve) => setTimeout(resolve, 1000))
})
