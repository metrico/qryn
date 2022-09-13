/* Qryn Tempo Query Handler */
/*
   Returns Protobuf-JSON formatted to /tempo/api/traces API
   Protobuf JSON Schema: https://github.com/metrico/qryn/pull/87#issuecomment-1003616559
   API Push Example: https://github.com/metrico/qryn/pull/87#issuecomment-1002683058

   TODO:
   - Refactor code and optimize interfacing with db/clickhouse.js and handler/tempo_push.js
   - Optimize for performance and reduce/remove POC debug layers

*/

const protoBuff = require('protobufjs')
const TraceDataType = protoBuff.loadSync(__dirname + '/../opentelemetry/proto/trace/v1/trace.proto')
  .lookupType('opentelemetry.proto.trace.v1.TracesData')

function pad (pad, str, padLeft) {
  if (typeof str === 'undefined') {
    return pad
  }
  if (padLeft) {
    return (pad + str).slice(-pad.length)
  } else {
    return (str + pad).substring(0, pad.length)
  }
}

function padLeft (size, str) {
  return pad((new Array(size)).fill('0').join(''), str, true)
}

async function handler (req, res) {
  req.log.debug('GET /api/traces/:traceId/:json')
  const jsonApi = req.params.json || false
  const resp = { data: [] }
  if (!req.params.traceId) {
    res.send(resp)
    return
  }
  if (req.params.traceId) {
    req.params.traceId = pad('00000000000000000000000000000000', req.params.traceId, true)
  }
  if (!req.params.traceId.match(/^[0-9a-fA-F]{32}$/) || req.params.traceId?.length !== 32) {
    res.code(400)
    return res.send(`invalid traceid ${req.params.traceId}`)
  }

  /* transpile trace params to logql selector */
  if (req.query.tags) {
    req.query.query = `{${req.query.tags}}`
    if (req.params.traceId) req.query.query += ` |~ "${req.params.traceId}"`
  } else if (this.tempo_tagtrace) {
    req.query.query = `{traceId="${req.params.traceId}"}`
  } else {
    req.query.query = `{type="tempo"} |~ "${req.params.traceId}"`
  }

  req.log.debug('Scan Tempo', req.query, req.params.traceId)

  /* TODO: handle time tag + direction + limit to govern the query output */
  try {
    const resp = await this.tempoQueryScan(
      req.query, res, req.params.traceId
    )
    /* Basic Structure for traces/v1 Protobuf encoder */
    const struct = { resourceSpans: [] }

    /* Reformat data from collected spans (includes overkill steps) */
    resp.v1.forEach(function (span) {
      struct.resourceSpans.push(formatSpanV1(span))
      req.log.debug({ span }, 'push span')
    })
    struct.resourceSpans.push.apply(struct.resourceSpans,
      resp.v2.map(span => formatSpanV2(span, jsonApi)))

    if (jsonApi) {
      /* Send spans into JSON response */
      req.log.debug({ struct }, 'PB-JSON')
      res.headers({ 'content-type': 'application/json' }).send(struct)
    } else {
      /* Pack spans into Protobuf response */
      const inside = TraceDataType.fromObject(struct)
      const proto = TraceDataType.encode(inside).finish()
      req.log.debug({ struct }, 'PB-JSON')
      req.log.debug({ proto: Buffer.from(proto).toString('hex') }, 'PB-HEX')
      res.header('Content-Type', 'application/x-protobuf')
      res.send(proto)
    }
  } catch (err) {
    req.log.error({ err })
    res.headers({ 'content-type': 'application/json' }).send(resp)
  }
}

function formatSpanV1 (span) {
  const attributes = []
  if (span.localEndpoint.serviceName || span.serviceName) {
    attributes.push({ key: 'service.name', value: { stringValue: span.localEndpoint.serviceName || span.serviceName } })
  }
  if (span.traceID) {
    const tmp = pad('00000000000000000000000000000000', span.traceID, true)
    span.traceId = Buffer.from(tmp, 'hex').toString('base64')
  }
  if (span.spanID) {
    const tmp = pad('0000000000000000', span.spanID, true)
    span.spanId = Buffer.from(tmp, 'hex').toString('base64')
  }
  if (span.parentSpanID) {
    var tmp = pad('0000000000000000', span.parentSpanID, true)
    span.parentSpanId = Buffer.from(tmp, 'hex').toString('base64')
  }
  if (span.operationName && !span.name) {
    span.name = span.operationName
  }
  if (span.tags.length > 0) {
    span.tags.forEach(function (tag) {
      attributes.push({ key: tag.key, value: { stringValue: tag.value || '' } })
    })
  }
  /* Form a new span/v1 Protobuf-JSON response object wrapper */
  var protoJSON = {
    resource: {
      attributes: [
        {
          key: 'collector',
          value: {
            stringValue: 'qryn'
          }
        }]
    },
    instrumentationLibrarySpans: [
      {
        instrumentationLibrary: {},
        spans: [span]
      }
    ]
  }
  /* Merge Attributes */
  if (attributes.length > 0) protoJSON.resource.attributes = protoJSON.resource.attributes.concat(attributes)
  /* Add to Protobuf-JSON struct */
  return protoJSON
}

function formatSpanV2 (span, json) {
  const getId = (rawId, size) => json ? rawId : Buffer.from(padLeft(size, rawId), 'hex').toString('base64')
  const res = {
    traceID: span.traceId,
    traceId: span.traceId ? getId(span.traceId, 32) : undefined,
    spanID: span.id,
    spanId: span.id ? getId(span.id, 16) : undefined,
    parentSpanId: span.parentId ? getId(span.parentId) : undefined,
    name: span.name || '',
    startTimeUnixNano: `${parseInt(span.timestamp)}000`,
    endTimeUnixNano: `${parseInt(span.timestamp) + parseInt(span.duration)}000`,
    attributes: [],
    events: (span.annotations || []).map(a => ({
      timeUnixNano: `${parseInt(a.timestamp)}000`,
      value: `${a.value || ''}`
    }))
  }
  const attrs = { ...span.tags, name: span.localEndpoint.serviceName }
  let serviceName = ''
  if (span.localEndpoint?.serviceName) {
    attrs['service.name'] = span.localEndpoint.serviceName
    serviceName = span.localEndpoint.serviceName
  } else if (span.remoteEndpoint?.serviceName) {
    attrs['service.name'] = span.remoteEndpoint.serviceName
    serviceName = span.remoteEndpoint.serviceName
  }
  res.attributes = Object.entries(attrs).map(e => ({ key: `${e[0]}`, value: { stringValue: `${e[1]}` } }))
  return {
    resource: {
      attributes: [
        {
          key: 'collector',
          value: {
            stringValue: 'qryn'
          }
        }, { key: 'service.name', value: { stringValue: serviceName } }]
    },
    instrumentationLibrarySpans: [
      {
        instrumentationLibrary: {},
        spans: [res]
      }
    ]
  }
}

module.exports = handler
