const fs = require('fs')
const path = require('path')
const { Compiler } = require('bnf')

const bnf = fs.readFileSync(path.join(__dirname, 'traceql.bnf')).toString()
const compiler = new Compiler()
compiler.AddLanguage(bnf, 'traceql')

module.exports = compiler
