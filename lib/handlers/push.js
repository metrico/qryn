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
const UTILS = require('../utils')
const stringify = require('json-stable-stringify')

function processStream (stream, labels, bulkLabels, bulk, toJSON, fingerPrint) {
  let finger = null
  let JSONLabels
  const promises = []
  if (stream.stream) {
    JSONLabels = stream.stream
  } else {
    JSONLabels = toJSON(
      stream.labels.replace(/\!?="/g, ':"')
    )
  }
  // Calculate Fingerprint
  const strJson = stringify(JSONLabels)
  finger = fingerPrint(strJson)
  // Store Fingerprint
  promises.push(bulkLabels.add([[
    new Date().toISOString().split('T')[0],
    finger,
    strJson,
    JSONLabels.name || ''
  ]]))
  labels.add(finger.toString(), finger.toString())
  for (const key in JSONLabels) {
    labels.add('_LABELS_', key)
    labels.add(key, JSONLabels[key])
  }
  const arrLabels = [...Object.entries(JSONLabels)]
  arrLabels.sort()
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
      values.push([
        finger,
        arrLabels,
        UTILS.parseStringifiedNanosOrRFC3339(entry.timestamp || entry.ts),
        (typeof entry.value === 'undefined') ? null : entry.value,
        entry.line || ''
      ])
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
      values.push([
        finger,
        arrLabels,
        BigInt(value[0]),
        (typeof value[2] === 'undefined') ? null : value[2],
        value[1] || ''
      ])
    })
    promises.push(bulk.add(values))
  }
  return Promise.all(promises).catch(console.log)
}

async function handler (req, res) {
  const self = this
  req.log.debug('POST /loki/api/v1/push')
  if (!req.body) {
    await processRawPush(req, self.labels, self.bulk_labels, self.bulk,
      self.toJSON, self.fingerPrint)
    res.code(200).send()
    return
  }
  if (this.readonly) {
    req.log.error('Readonly! No push support.')
    res.code(500).send()
    return
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
    req.log.debug({ streams }, 'GOT protoBuf')
  }
  const promises = []
  if (streams) {
    streams.forEach(function (stream) {
      promises.push(processStream(stream, self.labels, self.bulk_labels, self.bulk,
        self.toJSON, self.fingerPrint))
    })
    await Promise.all(promises)
  }
  res.code(204).send()
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
