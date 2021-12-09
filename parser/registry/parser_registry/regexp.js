const { Compiler } = require('bnf/Compiler')
const { unquote, addStream } = require('../common')
const Sql = require('clickhouse-sql')

const reBnf = `
<SYNTAX> ::= *(<literal> | <any_group>)
label ::= ( ALPHA | "_" ) *( ALPHA | DIGIT | "_" )
literal ::= <quoted_brack> | <letter>
quoted_brack ::= "\\(" | "\\)"
letter = !"\\(" !"\\)" !"(" !")" %x0-ff
group_name ::= "?" "<" <label> ">"
group_tail ::= *( <literal> | <any_group>)
any_group ::= "(" [<group_name>] <group_tail> ")"
`

const compiler = new Compiler()
compiler.AddLanguage(reBnf, 're')
/**
 *
 * @param token {Token}
 * @param res {{val: string, name?: string}[]}
 * @returns {{val: string, name?: string}[]}
 */
const walk = (token, res) => {
  res = res || []
  if (token.name === 'any_group') {
    if (token.tokens[1].name === 'group_name') {
      res.push({
        name: token.tokens[1].Child('label').value,
        val: token.tokens[2].value
      })
    } else {
      res.push({
        val: token.tokens.find(t => t.name === 'group_tail').value
      })
    }
  }
  for (const t of token.tokens) {
    res = walk(t, res)
  }
  return res
}

/**
 *
 * @param token {Token}
 * @returns {Token}
 */
const rmNames = (token) => {
  if (token.tokens) {
    token.tokens = token.tokens.filter(t => t.name !== 'group_name')
  }
  token.tokens.forEach(rmNames)
  return token
}

/**
 *
 * @param str {string}
 * @returns {Token}
 */
const compile = (str) => {
  const res = compiler.ParseScript(str, {}, 're')
  if (res === null) {
    throw new Error("can't compile")
  }
  return res.rootToken
}

/**
 *
 * @param regexp {string}
 * @returns {{labels: {val:string, name: string}[], re: string}}
 */
const extractRegexp = (regexp) => {
  const re = compile(unquote(regexp,
    null,
    (s) => s === '\\' ? '\\\\' : undefined))
  const labels = walk(re, [])
  const rmTok = rmNames(re)
  return {
    labels,
    re: rmTok.value
  }
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.viaRequest = (token, query) => {
  const { labels, re } = extractRegexp(token.Child('parameter').value)
  const namesArray = '[' + labels.map(l => `'${l.name}'` || '').join(',') + ']'
  query.select_list = query.select_list.filter(f => f[1] !== 'extra_labels')
  query.select([
    new Sql.Raw(`arrayFilter(x -> x.1 != '' AND x.2 != '', arrayZip(${namesArray}, ` +
      `arrayMap(x -> x[length(x)], extractAllGroupsHorizontal(string, '${re}'))))`),
    'extra_labels'])
  return query
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports.viaStream = (token, query) => {
  const re = new RegExp(unquote(token.Child('parameter').value))
  const getLabels = (m) => {
    return m && m.groups ? m.groups : {}
  }
  addStream(query, (s) => s.map(e => {
    return e.labels
      ? {
          ...e,
          labels: {
            ...e.labels,
            ...getLabels(e.string.match(re))
          }
        }
      : e
  }))
}

module.exports.internal = {
  rmNames: rmNames,
  walk: walk,
  compile: compile,
  extractRegexp: extractRegexp
}
