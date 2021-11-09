const { Compiler } = require('bnf/Compiler')
const { unquote } = require('../common')

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
const rm_names = (token) => {
  if (token.tokens) {
    token.tokens = token.tokens.filter(t => t.name !== 'group_name')
  }
  token.tokens.forEach(rm_names)
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
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.via_request = (token, query) => {
  const re = compile(unquote(token.Child('parameter').value,
    null,
    (s) => s === '\\' ? '\\\\' : undefined))
  const labels = walk(re, [])
  const rm_tok = rm_names(re)
  const names_array = '[' + labels.map(l => `'${l.name}'` || '').join(',') + ']'

  return {
    ...query,
    select: [
      ...query.select.filter(f => !f.endsWith('as extra_values')),
            `arrayFilter(x -> x.1 != '' AND x.2 != '', arrayZip(${names_array}, ` +
                `arrayMap(x -> x[length(x)], extractAllGroupsHorizontal(string, '${rm_tok.value}')))) as extra_labels`
    ]
  }
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.via_stream = (token, query) => {
  const re = new RegExp(unquote(token.Child('parameter').value))
  const getLabels = (m) => {
    return m && m.groups ? m.groups : {}
  }
  return {
    ...query,
    stream: [...(query.stream || []),
      (s) => s.map(e => {
        return e.labels
          ? {
              ...e,
              labels: {
                ...e.labels,
                ...getLabels(e.string.match(re))
              }
            }
          : e
      })
    ]
  }
}

module.exports.internal = {
  rm_names: rm_names,
  walk: walk,
  compile: compile
}
