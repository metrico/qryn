const hb = require('handlebars')
const { addStream, isEOF } = require('../common')
const { LineFmtOption } = require('../../../common')
const { compile } = require('./go_native_fmt')
const logger = require('../../../lib/logger')
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
  if (LineFmtOption() === 'go_native' || token.Child('line_format_fn').value === 'line_format_native') {
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
      if (!e) {
        return e
      }
      if (isEOF(e)) {
        processor.done()
        return e
      }
      if (!e.labels) {
        return e
      }
      if (!processor) {
        return null
      }
      if (processor.then) {
        try {
          await processor
        } catch (err) {
          processor = null
          logger.error({ err })
        }
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
