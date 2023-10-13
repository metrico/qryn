const { request } = require('./index');

(async () => {
  await new Promise((resolve => setTimeout(resolve, 1000)));
  const res = await request('test{}', Date.now() - 300000, Date.now(), 15000)
  console.log(res)
})()
