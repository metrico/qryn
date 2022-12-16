const { PluginLoaderBase } = require('plugnplay')
const { compileMongoQuery } = require('mongo-query-compiler')

/**

Mongodb-like JSON filtering output processor.
Usage: mongo({age: {$exists: true}}, {type="people"}

**/

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
    return this.query.match(/^mongo\(.+\)\s*$/)
  }

  /**
   * @method
   * @name process
   * @this {Plg}
   * @returns {Promise<{type: string, out: string}>} The raw output
   */
  async process () {
    const match = this.query.match(/^mongo\({(.+)},\s*(.+)\)$/)
    let response = await this.API.logql(match[2], this.start, this.end, this.limit)
    
    // Sanity Check. What error should this return?
    if (!match || !match[1] || !match[2]) return response;

    // Filter using Mongo Query Compiler
    let filterer = compileMongoQuery(match[1]);
    response.data.result = response.data.result.filter(filterer);
    return {
      type: 'application/json',
      out: response
    }
  }
}
class Plg extends PluginLoaderBase {
  exportSync (api) {
    return new Plugin()
  }
}

module.exports = Plg
