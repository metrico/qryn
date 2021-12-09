const { hashLabels, parseLabels } = require('../../common')
const { getPlg } = require('../../plugins/engine')
const Sql = require('clickhouse-sql')

/**
 * @param query {registry_types.Request | string[]}
 * @param clauses {string[]}
 * @returns {registry_types.Request | string[]}
 */
module.exports._and = (query, clauses) => {
  if (Array.isArray(query)) {
    if (!query.length) {
      return ['AND', ...clauses]
    }
    return query[0] === 'AND'
      ? [...query, ...clauses]
      : ['AND', query, ...clauses]
  }
  query = { ...query }
  if (!query.where) {
    query.where = ['AND']
  } else if (query.where[0] !== 'AND') {
    query.where = ['AND', query.where]
  } else {
    query.where = [...query.where]
  }
  query.where.push.apply(query.where, clauses)
  return query
}

/**
 *
 * @param query {Select}
 * @returns {DataStream[]}
 */
module.exports.getStream = (query) => {
  return query && query.ctx && query.ctx.stream ? query.ctx.stream : []
}

/**
 *
 * @param query {Select}
 * @returns {boolean}
 */
module.exports.hasStream = (query) => {
  return module.exports.getStream(query).length > 0
}

/**
 *
 * @param query {Select}
 * @param stream {function(DataStream): DataStream}
 * @returns {Select}
 */
module.exports.addStream = (query, stream) => {
  if (!query) {
    throw new Error('query is undefined')
  }
  if (query && query.ctx && query.ctx.stream) {
    query.ctx.stream.push(stream)
    return query
  }
  if (query && query.ctx) {
    query.ctx.stream = [stream]
    return query
  }
  query.ctx = { stream: [stream] }
  return query
}

/**
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.querySelectorPostProcess = (query) => {
  return query
}

/**
 *
 * @param token {Token}
 * @returns {string}
 */
module.exports.unquoteToken = (token) => {
  let value = token.Child('quoted_str').value
  value = `"${value.substr(1, value.length - 2)}"`
  return JSON.parse(value)
}

/**
 *
 * @param durationStr {string}
 * @returns {number}
 */
module.exports.durationToMs = (durationStr) => {
  const durations = {
    ns: 1 / 1000000,
    us: 1 / 1000,
    ms: 1,
    s: 1000,
    m: 60000,
    h: 60000 * 60
  }
  for (const k of Object.keys(durations)) {
    const m = durationStr.match(new RegExp(`^([0-9][.0-9]*)${k}$`))
    if (m) {
      return parseInt(m[1]) * durations[k]
    }
  }
  throw new Error('Unsupported duration')
}

/**
 *
 * @param s {DataStream}
 * @param fn
 * @returns {DataStream}
 */
module.exports.map = (s, fn) => s.map((e) => {
  return new Promise((resolve) => {
    setImmediate(() => {
      resolve(fn(e))
    })
  })
})

/**
 *
 * @param token {Token}
 * @returns {number}
 */
module.exports.getDuration = (token) => {
  return module.exports.durationToMs(token.Child('duration_value').value)
  // Math.max(duration, query.ctx && query.ctx.step ? query.ctx.step : 1000);
}

const getDuration = module.exports.getDuration

/**
 *
 * @param eof {any}
 * @returns boolean
 */
module.exports.isEOF = (eof) => eof.EOF

/**
 *
 * @param type {string}
 * @param cb {(function(any): any) | undefined}
 * @returns {Object<string, (function(any): any)>}
 */
module.exports.getPlugins = (type, cb) => {
  const _plgs = getPlg({ type: type })
  const plgs = {}
  for (const _e of Object.values(_plgs)) {
    for (const e of Object.entries(_e)) {
      plgs[e[0]] = cb ? cb(e[1]) : () => e[1]
    }
  }
  return plgs
  /* for (let file of glob.sync(path + "/*.js")) {
        const mod = require(file);
        for (let fn of Object.keys(mod)) {
            plugins[fn] = cb ? cb(mod[fn]()) : mod[fn]();
        }
    }
    return plugins; */
}

/**
 *
 * @param query {Select}
 * @returns {boolean}
 */
module.exports.hasExtraLabels = (query) => {
  return query.select().some(f => f[1] === 'extra_labels')
}

/**
 *
 * @param query {Select}
 * @returns {SQLObject}
 */
module.exports.concatLabels = (query) => {
  if (module.exports.hasExtraLabels(query)) {
    return new Sql.Raw('arraySort(arrayConcat(arrayFilter(' +
            'x -> arrayExists(y -> y.1 == x.1, extra_labels) == 0, ' +
            'JSONExtractKeysAndValues(labels, \'String\')), extra_labels))')
  }
  return new Sql.Raw('JSONExtractKeysAndValues(labels, \'String\')')
}

/**
 * sum_over_time(unwrapped-range): the sum of all values in the specified interval.
 * @param token {Token}
 * @param query {Select}
 * @param byWithoutName {string} name of the by_without token
 * @returns {Select}
 */
function applyByWithoutStream (token, query, byWithoutName) {
  const isBy = token.Child(byWithoutName).value === 'by'
  const filterLabels = token.Children('label').map(l => l.value)
  return module.exports.addStream(query,
    /**
   *
   * @param stream {DataStream}
   */
    (stream) => stream.map(e => {
      if (!e || !e.labels) {
        return e
      }
      const labels = [...Object.entries(e.labels)].filter(l =>
        (isBy && filterLabels.includes(l[0])) || (!isBy && !filterLabels.includes(l[0]))
      )
      return { ...e, labels: parseLabels(labels) }
    }))
}

/**
 *
 * @param values {Object}
 * @param timestamp {number}
 * @param value {number}
 * @param duration {number}
 * @param step {number}
 * @param counterFn {function(any, any, number): any}
 * @returns {Object}
 */
function addTimestamp (values, timestamp, value, duration, step, counterFn) {
  const timestampWithoutStep = Math.floor(timestamp / duration) * duration
  const timestampWithStep = step > duration
    ? Math.floor(timestampWithoutStep / step) * step
    : timestampWithoutStep
  if (!values) {
    values = {}
  }
  if (!values[timestampWithStep]) {
    values[timestampWithStep] = {}
  }
  if (!values[timestampWithStep][timestampWithoutStep]) {
    values[timestampWithStep][timestampWithoutStep] = 0
  }
  values[timestampWithStep][timestampWithoutStep] =
        counterFn(values[timestampWithStep][timestampWithoutStep], value, timestamp)
  return values
}

/**
 *
 * @param query {Select}
 * @returns {boolean}
 */
module.exports.hasExtraLabels = (query) => {
  return query.select().some((x) => x[1] === 'extra_labels')
}

/**
 *
 * @param token {Token}
 * @param query {Select}
 * @param counterFn {function(any, any, number): any}
 * @param summarizeFn {function(any): number}
 * @param lastValue {boolean} if the applier should take the latest value in step (if step > duration)
 * @param byWithoutName {string} name of the by_without token
 * @returns {Select}
 */
module.exports.applyViaStream = (token, query,
  counterFn, summarizeFn, lastValue, byWithoutName) => {
  query.ctx.matrix = true
  byWithoutName = byWithoutName || 'by_without'
  if (token.Child(byWithoutName)) {
    query = applyByWithoutStream(token.Child(`opt_${byWithoutName}`), query, byWithoutName)
  }
  let results = new Map()
  const duration = getDuration(token, query)
  query.ctx.duration = duration
  const step = query.ctx.step
  /**
  * @param s {DataStream}
  */
  const stream = (s) => s.remap((emit, e) => {
    if (!e || !e.labels) {
      for (const v of results.values()) {
        const ts = [...Object.entries(v.values)]
        ts.sort()
        for (const _v of ts) {
          let value = Object.entries(_v[1])
          value.sort()
          value = lastValue ? value[value.length - 1][1] : value[0][1]
          value = summarizeFn(value)// Object.values(_v[1]).reduce((sum, v) => sum + summarizeFn(v), 0);
          emit({ labels: v.labels, timestamp_ms: _v[0], value: value })
        }
      }
      results = new Map()
      emit({ EOF: true })
      return
    }
    const l = hashLabels(e.labels)
    if (!results.has(l)) {
      results.set(l, {
        labels: e.labels,
        values: addTimestamp(undefined, e.timestamp_ms, e, duration, step, counterFn)
      })
    } else {
      results.get(l).values = addTimestamp(
        results.get(l).values, e.timestamp_ms, e, duration, step, counterFn
      )
    }
  })
  return module.exports.addStream(query, stream)
}

/**
 *
 * @param str {string}
 * @param custom {(function(string): string | undefined) | undefined}
 * @param customSlash {(function(string): (string | undefined)) | undefined}
 * @return {string}
 */
module.exports.unquote = (str, custom, customSlash) => {
  const quote = str.substr(0, 1)
  switch (quote) {
    case '"':
    case '`':
      break
    default:
      throw new Error(`Unknown quote: ${quote}`)
  }
  str = str.trim()
  str = str.substr(1, str.length - 2)
  let res = ''
  let slash = false
  for (let i = 0; i < str.length; i++) {
    if (!slash) {
      if (custom && custom(str[i])) {
        res += custom(str[i])
        continue
      }
      if (str[i] === quote) {
        throw new Error('Unexpected quote')
      }
      switch (str[i]) {
        case '\\':
          slash = true
          continue
        default:
          res += str[i]
      }
    }
    if (slash) {
      slash = false
      if (customSlash && customSlash(str[i])) {
        res += customSlash(str[i])
        continue
      }
      if (str[i] === quote) {
        res += quote
        continue
      }
      switch (str[i]) {
        case 'r':
          res += '\r'
          break
        case 'n':
          res += '\n'
          break
        case 't':
          res += '\t'
          break
        default:
          res += '\\' + str[i]
      }
    }
  }
  return res
}

module.exports.sharedParamNames = {
  samplesTable: 'samplesTable',
  timeSeriesTable: 'timeSeriesTable',
  from: 'from',
  to: 'to',
  limit: 'limit'
}
