const { scanSeries } = require('../db/clickhouse')
const { CORS } = require('../../common')
const { Compiler } = require('bnf')
const { isArray } = require('handlebars-helpers/lib/array')
const { QrynError } = require('./errors')

const promqlSeriesBnf = `
<SYNTAX> ::= <metric_name><OWSP> | "{" <OWSP> <label_selectors> <OWSP>  "}" | <metric_name><OWSP> "{" <OWSP> [<label_selectors>] <OWSP> "}"
label ::= (<ALPHA> | "_") *(<ALPHA> | "." | "_" | <DIGITS>)
operator ::= "=~" | "!~" | "!=" | "="
quoted_str ::= (<QUOTE><QUOTE>) | (<AQUOTE><AQUOTE>) | <QLITERAL> | <AQLITERAL>
metric_name ::= label
label_selector ::= <label> <OWSP> <operator> <OWSP> <quoted_str>
label_selectors ::= <label_selector><OWSP>*(","<OWSP><label_selector><OWSP>)
`

const compiler = new Compiler()
compiler.AddLanguage(promqlSeriesBnf, 'promql_series')

// Series Handler
async function handler (req, res) {
  if (req.method === 'POST') {
    req.query = req.body
  }
  let query = req.query.match || req.query['match[]']
  // bypass queries unhandled by transpiler
  if (query.includes('node_info')) {
    return res.send({ status: 'success', data: [] })
  }
  if (!isArray(query)) {
    query = [query]
  }
  query = query.map((m) => {
    let res = '{'
    let parsed = compiler.ParseScript(m)
    if (!parsed) {
      throw new QrynError(400, `invalid series query ${m}`)
    }
    parsed = parsed.rootToken
    res += parsed.Child('metric_name') ? `name="${parsed.Child('metric_name').val}` : ''
    res += parsed.Child('metric_name') && parsed.Child('label_selector') ? ',' : ''
    res += parsed.Children('label_selector').map(c => c.value.toString()).join(',')
    res += '}'
    return res
  })
  // convert the input query into a label selector
  const response = await scanSeries(query)
  res.code(200)
  res.headers({
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': CORS
  })
  return response
}

module.exports = handler
