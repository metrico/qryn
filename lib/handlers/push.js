/* Push Handler */
/*
    Accepts JSON formatted requests when the header Content-Type: application/json is sent.
    Example of the JSON format:
    {
      "streams": [
          {
              "labels": "{foo=\"bar\"}",
              "entries": [
                  {"ts": "2018-12-18T08:28:06.801064-04:00", "line": "baz"}
                ]
              }
            ]
          }
*/

const { chain } = require('stream-chain')
const { parser } = require('stream-json')
const { Transform } = require('stream')
const FilterBase = require('stream-json/filters/FilterBase')
const StreamValues = require('stream-json/streamers/StreamValues')
const logger = require('../logger')
const UTILS = require('../utils')
const DATABASE = require('../db/clickhouse')
const { asyncLogError, logType, metricType, bothType } = require('../../common')
const stringify = UTILS.stringify
const fingerPrint = UTILS.fingerPrint
const { bulk_labels, bulk, labels } = DATABASE.cache
const toJson = UTILS.toJSON
const { readonly } = require('../../common')

function processStream (stream, labels, bulkLabels, bulk, toJSON, fingerPrint) {
  let finger = null
  let JSONLabels
  const promises = []
  if (stream.stream) {
    JSONLabels = stream.stream
  } else {
    JSONLabels = toJSON(
      stream.labels //stream.labels.replace(/\!?="/g, ':"')
    )
  }
  // Calculate Fingerprint
  const strJson = stringify(JSONLabels)
  let type = 3
  finger = fingerPrint(strJson)
  labels.add(finger.toString(), finger.toString())
  for (const key in JSONLabels) {
    labels.add('_LABELS_', key)
    labels.add(key, JSONLabels[key])
  }
  const dates = {}
  if (stream.entries) {
    const values = []
    stream.entries.forEach(function (entry) {
      if (
        !entry &&
        (!entry.timestamp || !entry.ts) &&
        (!entry.value || !entry.line)
      ) {
        console.error('no bulkable data', entry)
        return
      }
      if (!entry.value) {
        type &= logType
      }
      if (!entry.line || entry.line === '') {
        type &= metricType
      }
      const ts = UTILS.parseStringifiedNanosOrRFC3339(entry.timestamp || entry.ts)
      values.push([
        finger,
        ts,
        (typeof entry.value === 'undefined') ? null : entry.value,
        entry.line || '',
        type === 3 ? bothType : type
      ])
      dates[new Date(Number(ts / BigInt(1000000))).toISOString().split('T')[0]] = true
    })
    promises.push(bulk.add(values))
  }
  if (stream.values) {
    const values = []
    stream.values.forEach(function (value) {
      if (!value && !value[0] && !value[1]) {
        console.error('no bulkable data', value)
        return
      }

      if (typeof value[2] === 'undefined') {
        type &= logType
      }
      if (!value[1]) {
        type &= metricType
      }

      const ts = BigInt(value[0])
      values.push([
        finger,
        BigInt(value[0]),
        (typeof value[2] === 'undefined') ? null : value[2],
        value[1] || '',
        type === 3 ? bothType : type
      ])
      dates[new Date(Number(ts / BigInt(1000000))).toISOString().split('T')[0]] = true
    })
    promises.push(bulk.add(values))
  }
  for (const date of Object.keys(dates)) {
    // Store Fingerprint
    promises.push(bulkLabels.add([[
      date,
      finger,
      strJson,
      JSONLabels.name || '',
      type === 3 ? bothType : type
    ]]))
  }
  return Promise.all(promises)
}

async function handler (req, res) {
  req.log.debug('POST /loki/api/v1/push')
  if (!req.body) {
    await processRawPush(req, DATABASE.cache.labels, bulk_labels, bulk,
      toJson, fingerPrint)
    return res.code(200).send()
  }
  if (readonly) {
    asyncLogError('Readonly! No push support.', req.log)
    return res.code(500).send()
  }
  let streams
  if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/json') > -1
  ) {
    streams = req.body.streams
  } else if (
    req.headers['content-type'] &&
                req.headers['content-type'].indexOf('application/x-protobuf') > -1
  ) {
    // streams = messages.PushRequest.decode(req.body)
    streams = req.body
  }
  const promises = []
  if (streams) {
    streams.forEach(function (stream) {
      promises.push(processStream(stream,
        DATABASE.cache.labels, DATABASE.cache.bulk_labels, DATABASE.cache.bulk,
        UTILS.toJSON, fingerPrint))
    })
    await Promise.all(promises)
  }
  return res.code(204).send()
}

class StackChecker extends FilterBase {
  _checkChunk (chunk) {
    /**/
    return false
  }

  _check (chunk, _, callback) {
    const self = this
    return super._check(chunk, _, () => {
      this.push({
        ...chunk,
        stack: self._stack.filter(s => s !== null && typeof s !== 'undefined').join('.')
      })
      callback(null)
    })
  }
}

class ConditionalStreamValues extends StreamValues {
  constructor (options) {
    super()
    this.filter = options.filter
    this.id = options.id
    this.syncPasses = 0
  }

  __transform (chunk, encoding, callback) {
    if (!chunk) {
      this.pass(chunk, encoding)
      callback(null)
      return
    }
    if (chunk.isProcessed) {
      this.pass(chunk, encoding)
      callback(null)
      return
    }
    this.stack = chunk.stack
    if (!this.filter.test(chunk.stack)) {
      this.pass(chunk, encoding)
      callback(null)
      return
    }
    super._transform(chunk, encoding, callback)
  }

  _transform (chunk, encoding, callback) {
    this.__transform(chunk, encoding, callback)
  }

  pass (chunk, encoding) {
    return super.push(chunk, encoding)
  }

  push (chunk, encoding) {
    if (!chunk) {
      return super.push(chunk, encoding)
    }
    if (!chunk.value) {
      return
    }
    return super.push({
      ...chunk,
      stack: this.stack,
      isProcessed: this.id
    }, encoding)
  }
}

const processRawPush = async (req, labels, bulkLabels, bulkValues, toJSON, fingerPrint) => {
  let stream = null
  const promises = []
  const addPromise = () => {
    if (stream && (stream.values || stream.entries)) {
      const idx = promises.length
      promises.push(processStream({ ...stream }, labels, bulkLabels, bulkValues, toJSON, fingerPrint)
        .then(() => { promises[idx] = null }, (err) => { promises[idx] = err }))
      stream = { ...stream, values: [] }
    }
  }
  const pipeline = chain([
    req.raw,
    parser(),
    new StackChecker(),
    new Transform({
      objectMode: true,
      transform: function (chunk, encoding, callback) {
        if (chunk && chunk.name === 'startObject' &&
          /^streams\.\d+$/.test(chunk.stack)) {
          addPromise()
          stream = {}
        }
        callback(null, chunk)
      }
    }),
    new ConditionalStreamValues({ filter: /^streams\.\d+\.stream/, id: 'stream' }),
    new ConditionalStreamValues({ filter: /^streams\.\d+\.values\.\d+/, id: 'values' })
  ])
  let size = 0
  pipeline.on('data', data => {
    switch (data.isProcessed) {
      case 'stream':
        stream = { stream: data.value }
        break
      case 'values':
        if (!stream) {
          throw new Error('labels undefined')
        }
        stream.values = stream.values || []
        stream.values.push(data.value)
        size += data.value[0].toString().length +
          data.value[1].toString().length +
          (data.value[2] ? data.value[2].toString().length : 0)
        if (size > 100000) {
          addPromise()
          size = 0
        }
    }
  })
  await new Promise((resolve, reject) => {
    pipeline.once('end', resolve)
    pipeline.once('error', reject)
  })
  const err = promises.find(p => p instanceof Error)
  if (err) {
    throw err
  }
  await Promise.all(promises.filter(p => p))
}

module.exports = handler
