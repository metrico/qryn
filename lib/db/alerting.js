const transpiler = require('../../parser/transpiler')
const { dropAlertViews, createAlertViews, incAlertMark, dropOutdatedParts, getLastAlert, getAlertRules } = require('./clickhouse')
const { samplesTableName } = require('../utils')

module.exports.addAlert = async (name, request, labels) => {
  const watcher = new AlertWatcher(name, request, labels)
  await watcher.run()
  alerts[name] = watcher
}

module.exports.editAlert = async (name, request, labels) => {
  if (!alerts[name]) {
    await module.exports.addAlert(name, request, labels)
    return
  }
  await alerts[name].edit(request, labels)
}

module.exports.dropAlert = async (name) => {
  alerts[name] && await alerts[name].drop()
}

module.exports.stop = () => {
  Object.values(alerts).forEach(a => a.stop())
  alerts = {}
}

module.exports.startAlerting = async () => {
  const rules = await getAlertRules()
  for (const rule of rules) {
    alerts[rule.name] = new AlertWatcher(rule.name, rule.request, rule.labels)
    await alerts[rule.name].run()
  }
}

/**
 *
 * @type {Object<string, AlertWatcher>}
 */
let alerts = {}

class AlertWatcher {
  constructor (name, request, labels) {
    this.name = name
    this.request = request
    this.labels = labels
  }

  async run () {
    await this._createViews()
    this.interval = setInterval(() => {
      this._checkViews().catch(console.log)
    }, 1000)
  }

  async edit (request, labels) {
    if (this.request !== request) {
      this.request = request
      await this._dropViews()
      await this._createViews()
    }
    this.labels = labels
  }

  async drop () {
    this.stop()
    await this._dropViews()
  }

  stop () {
    if (this.interval) {
      clearInterval(this.interval)
    }
  }

  _dropViews () {
    return dropAlertViews(this.name)
  }

  async _createViews () {
    /**
     *
     * @type {{query: Select, stream: (function(DataStream): DataStream)[]}}
     */
    const query = transpiler.transpileTail({
      query: this.request,
      samplesTable: samplesTableName,
      rawRequest: true,
      suppressTime: true
    })
    query.query.order_expressions = []
    return createAlertViews(this.name, query.query)
  }

  async _checkViews () {
    const mark = await incAlertMark(this.name)
    const lastAlert = await getLastAlert(this.name, mark)
    console.log(lastAlert)
    await dropOutdatedParts(this.name, mark)
  }
}
