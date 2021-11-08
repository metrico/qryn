const { readFileSync, writeFileSync } = require('fs')

let content = readFileSync(process.argv[2], { encoding: 'utf8' })
const re = new RegExp(`^exports\\[\`${process.argv[3]}\`\\] = \`(\n([^e].*)$)+\n\nexports\\[`, 'gm')
content = content.replace(re, 'exports[')
writeFileSync(process.argv[2] + '_', content)
