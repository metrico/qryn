const DATABASE = require('../db/clickhouse')
const { asyncLogError, logType, metricType, bothType, readonly } = require('../../common')
const UTILS = require('../utils')
const stringify = UTILS.stringify
const fingerPrint = UTILS.fingerPrint
const { bulk_labels, bulk, labels } = DATABASE.cache

async function handle (req, res) {
  if (readonly) {
    asyncLogError('Readonly! No push support.', req.log)
    return res.code(500).send()
  }
  try {
    const promises = []
    const fingerprints = {}
    for (const resourceLogsEntry of req.body.resourceLogs) {
      const resAttrs = resource2Attrs(resourceLogsEntry.resource)
      for (const scopeLogsEntry of resourceLogsEntry.scopeLogs) {
        const scopeAttrs = {
          ...resAttrs,
          ...resource2Attrs(scopeLogsEntry.scope)
        }
        for (const logRecord of scopeLogsEntry.logRecords) {
          const logAttrs = {
            ...scopeAttrs,
            ...resource2Attrs(logRecord)
          }
          if (logRecord.severityText) {
            logAttrs.level = logRecord.severityText
          }
          const labels = stringify(logAttrs)
          const fingerprint = fingerPrint(labels)
          const ts = BigInt(logRecord.timeUnixNano)
          promises.push(bulk.add([[
            fingerprint,
            ts,
            null,
            anyValueToString(logRecord.body),
            logType
          ]]))
          const date = new Date(Number(ts / BigInt(1000000))).toISOString().split('T')[0]
          !fingerprints[fingerprint] && promises.push(bulk_labels.add([[
            date,
            fingerprint,
            labels,
            labels.name || '',
            logType
          ]]))
          fingerprints[fingerprint] = true
        }
      }
    }
    await Promise.all(promises)
  } catch (error) {
    await asyncLogError(error, req.log)
    res.status(500).send({ error: 'Internal Server Error' })
  }
}

function resource2Attrs (resource) {
  if (!resource || !resource.attributes) {
    return {}
  }
  const attrs = {}
  for (const attribute of resource.attributes) {
    attrs[normalizeAttrName(attribute.key)] = anyValueToString(attribute.value)
  }
  return attrs
}

function normalizeAttrName (name) {
  return name.replaceAll(/[^a-zA-Z0-9_]/g, '_')
}

function anyValueToString (value) {
  if (!value) {
    return ''
  }
  if (value.stringValue) {
    return value.stringValue
  }
  if (value.boolValue) {
    return value.boolValue ? 'true' : 'false'
  }
  if (value.intValue) {
    return value.intValue.toString()
  }
  if (value.doubleValue) {
    return value.doubleValue.toString()
  }
  if (value.bytesValue) {
    return Buffer.from(value.bytesValue).toString('base64')
  }
  if (value.arrayValue) {
    return JSON.stringify(value.arrayValue.values.map(anyValueToString))
  }
  if (value.kvlistValue) {
    return JSON.stringify(value.kvlistValue.values.reduce((agg, pair) => ({
      ...agg,
      [pair.key]: anyValueToString(pair.value)
    })))
  }
  return ''
}

module.exports = handle
