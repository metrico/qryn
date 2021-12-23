const hb = require('handlebars')
const { addStream, isEOF } = require('../common')
const { LineFmtOption } = require('../../../common')
const { compile } = require('./go_native_fmt')
require('handlebars-helpers')(['math', 'string'], {
  handlebars: hb
})

/**
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports = (token, query) => {
  let processor = null
  const fmt = JSON.parse('"' + token.Child('quoted_str').value.replace(/(^"|^'|"$|'$)/g, '') + '"')
  if (LineFmtOption() === 'go_native') {
    processor = compile(fmt)
    processor.then((p) => {
      processor = p
    })
  } else {
    processor = {
      process: hb.compile(fmt),
      done: () => {}
    }
  }
  return addStream(query,
    /**
     *
     * @param s {DataStream}
     */
    (s) => s.map(async (e) => {
      if (isEOF(e)) {
        processor.done()
      }
      if (!e.labels) {
        return e
      }
      if (processor.then) {
        await processor
      }
      try {
        const res = processor.process({ ...e.labels, _entry: e.string })
        return {
          ...e,
          string: res
        }
      } catch (err) {
        return null
      }
    }).filter(e => e))
}
