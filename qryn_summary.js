const parser = require('./parser/transpiler')
const clickhouse = require('./lib/db/clickhouse')
const wasm = require('./wasm_parts/main')
wasm.ResetWasm = false;
const store = async () => {
  await clickhouse.init('cloki')
  await clickhouse.checkDB()
  const end = Math.floor(Date.now() / 600000) * 600000000000
  const start = end - 600000000000
  const req = parser.transpileSummaryETL({
    query: process.env.SUMMARY,
    start,
    end,
    limit: 100000
  })
  const resp = await clickhouse.rawRequest(
    req.query + ' FORMAT JSONEachRow',
    null,
    clickhouse.databaseOptions.queryOptions.database
  )
  const fpToCtx = {}
  resp.data
    .split('\n')
    .filter(l => l)
    .map(l => {
      return JSON.parse(l)
    }).forEach(l => {
      if (!fpToCtx[l.fingerprint]) {
        fpToCtx[l.fingerprint] = wasm.startSummary()
      }
      wasm.summaryLogs(fpToCtx[l.fingerprint], [l.string])
    })

  for (const [fp, id] of Object.entries(fpToCtx)) {
    const patterns = wasm.getSummary(id)
    await clickhouse.rawRequest('INSERT INTO patterns FORMAT JSONEachRow',
      patterns.map(p => JSON.stringify({
        fingerprint: fp,
        timestamp_s: Math.floor(end / 1000000000),
        pattern_key_level: p.key.level,
        pattern_hash: p.key.hash,
        sample: p.pattern.sample,
        messages: p.pattern.messages,
        pattern_words: p.pattern.pattern.words,
        pattern_string: p.pattern.pattern.str
      })).join('\n'),
      clickhouse.databaseOptions.queryOptions.database)
  }
  wasm.reinit()
}

setTimeout(store, 600000)
