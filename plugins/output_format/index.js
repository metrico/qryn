const { PluginLoaderBase } = require('plugnplay')

/**
 * @class Plugin
 * @property {string} query
 * @property start {number} start in NS
 * @property end {string} end in NS
 * @property type {string} promql or logql
 * @property limit {number}
 * @property {{
 *   logql: (query: string, startNS: number, endNS: number, limit: number) => Promise<Object>
 * }} API
 *   promql: (query: string, startNS: number, endNS: number, limit: number) => Promise<Object> //not implemented
 */
class Plugin {
  /**
   * @method
   * @name check
   * @this {Plg}
   * @returns {boolean} if this plugin is usable for the query
   */
  check () {
    return this.query.match(/^toCsv\(.+\)\s*$/)
  }

  /**
   * @method
   * @name process
   * @this {Plg}
   * @returns {Promise<{type: string, out: string}>} The raw output
   */
  async process () {
    const match = this.query.match(/^toCsv\((.+)\)$/)
    const response = await this.API.logql(match[1], this.start, this.end, this.limit)
    let res = ''
    for (const stream of response.data.result) {
      const labels = JSON.stringify(stream.stream)
      for (const val of stream.values) {
        res += `${labels}\t${val[0]}\t${val[1]}\n`
      }
    }
    return {
      type: 'text/csv',
      out: res
    }
  }
}
class Plg extends PluginLoaderBase {
  exportSync (api) {
    return new Plugin()
  }
}

module.exports = Plg
