const { createPoints, sendPoints } = require('./common')

const period = 1000 // process.argv[1]
const time = 600000 // process.argv[2]
const id = '_TEST_' // process.argv[3];

let cnt = 0

async function main () {
  console.log('started')
  const t = setInterval(() => {
    cnt++
    const points = createPoints(id, 1, Date.now() - 1000, Date.now(), {}, {},
      () => `MSG_${cnt}`)
    sendPoints('http://localhost:3100', points)
  }, period)

  await new Promise((resolve, reject) => setTimeout(resolve, time))
  clearInterval(t)
  console.log('end')
}

main()
