const hb = require('handlebars')
const { addStream } = require('./common')
require('handlebars-helpers')(['math', 'string'], {
  handlebars: hb
})

/**
 * @param token {Token}
 * @param query {Select}
 * @returns {Select}
 */
module.exports = (token, query) => {
  const fmt = JSON.parse('"' + token.Child('quoted_str').value.replace(/(^"|^'|"$|'$)/g, '') + '"')
  const processor = hb.compile(fmt)
  return addStream(query,
    /**
     *
     * @param s {DataStream}
     */
    (s) => s.map((e) => {
      if (!e.labels) {
        return e
      }
      try {
        return {
          ...e,
          string: processor({ ...e.labels, _entry: e.string })
        }
      } catch (err) {
        return null
      }
    }).filter(e => e))
}
