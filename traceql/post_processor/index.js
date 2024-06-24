const Zipkin = require('../../lib/db/zipkin')
const { flatOTLPAttrs, OTLPgetServiceNames } = require('../../lib/utils')
/**
 *
 * @param rows {Row[]}
 * @param script {Token}
 */
function postProcess (rows, script) {
  const selectAttrs = script.Children('aggregator')
    .filter(x => x.Child('fn').value === 'select')
    .map(x => x.Children('label_name'))
    .reduce((acc, x) => {
      let attrs = x.map(y => ({
        name: y.value,
        path: y.value.split('.').filter(y => y)
      }))
      if (attrs[0] === 'span' || attrs[0] === 'resource') {
        attrs = attrs.slice(1)
      }
      return [...acc, ...attrs]
    }, [])
  rows = rows.map(row => ({
    ...row,
    objs: row.payload.map((payload, i) => {
      let span = null
      let attrs = null
      let serviceName = null

      switch (row.payload_type[i]) {
        case 1:
          return new Zipkin(JSON.parse(Buffer.from(payload, 'base64').toString()))
        case 2:
          span = JSON.parse(Buffer.from(payload, 'base64').toString())
          attrs = flatOTLPAttrs(span.attributes)
          serviceName = OTLPgetServiceNames(attrs)
          attrs.name = span.name
          attrs['service.name'] = serviceName.local
          if (serviceName.remote) {
            attrs['remoteService.name'] = serviceName.remote
          }
          attrs = [...Object.entries(attrs)]
          return { tags: attrs }
      }
      return null
    })
  }))
  const spans = (row) => row.span_id.map((spanId, i) => ({
    spanID: spanId,
    startTimeUnixNano: row.timestamp_ns[i],
    durationNanos: row.duration[i],
    attributes: selectAttrs.map(attr => ({
      key: attr.name,
      value: {
        stringValue: (row.objs[i].tags.find(t => t[0] === attr.path.join('.')) || [null, null])[1]
      }
    })).filter(x => x.value.stringValue)
  }))
  const traces = rows.map(row => ({
    traceID: row.trace_id,
    rootServiceName: row.root_service_name,
    rootTraceName: row.root_trace_name,
    startTimeUnixNano: row.start_time_unix_nano,
    durationMs: row.duration_ms,
    spanSet: { spans: spans(row) },
    spanSets: [
      {
        spans: spans(row),
        matched: row.span_id.length
      }
    ]
  }))
  return traces
}

module.exports = {
  postProcess
}
