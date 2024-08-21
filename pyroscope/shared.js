const { QrynBadRequest } = require('../lib/handlers/errors')
const Sql = require('@cloki/clickhouse-sql')
const compiler = require('../parser/bnf')
/**
 *
 * @param payload {ReadableStream}
 * @returns {Promise<Buffer>}
 */
const bufferize = async (payload) => {
  const _body = []
  payload.on('data', data => {
    _body.push(data)// += data.toString()
  })
  if (payload.isPaused && payload.isPaused()) {
    payload.resume()
  }
  await new Promise(resolve => {
    payload.on('end', resolve)
    payload.on('close', resolve)
  })
  const body = Buffer.concat(_body)
  if (body.length === 0) {
    return null
  }
  return body
}

const parser = (MsgClass) => {
  return async (req, payload) => {
    const body = await bufferize(payload)
    req._rawBody = body
    return MsgClass.deserializeBinary(body)
  }
}

/**
 *
 * @param proto {Object}
 */
const normalizeProtoResponse = (proto) => {
  if (typeof proto !== 'object') {
    return proto
  }
  return Object.fromEntries(Object.entries(proto).map((e) => {
    let name = e[0]
    if (name.endsWith('List')) {
      name = name.slice(0, -4)
    }
    if (Array.isArray(e[1])) {
      return [name, e[1].map(normalizeProtoResponse)]
    }
    if (typeof e[1] === 'object') {
      return [name, normalizeProtoResponse(e[1])]
    }
    return [name, e[1]]
  }))
}

const wrapResponse = (hndl) => {
  return async (req, res) => {
    const _res = await hndl(req, res)
    if (!_res || !_res.serializeBinary) {
      return _res
    }
    if (req.type === 'json') {
      const strRes = JSON.stringify(normalizeProtoResponse(_res.toObject()))
      return res.code(200).send(strRes)
    }
    return res.code(200).send(Buffer.from(_res.serializeBinary()))
  }
}

const serviceNameSelectorQuery = (labelSelector) => {
  const empty = Sql.Eq(new Sql.Raw('1'), new Sql.Raw('1'))
  if (!labelSelector || !labelSelector.length || labelSelector === '{}') {
    return empty
  }
  const labelSelectorScript = parseLabelSelector(labelSelector)
  let conds = null
  for (const rule of labelSelectorScript.Children('log_stream_selector_rule')) {
    const label = rule.Child('label').value
    if (label !== 'service_name') {
      continue
    }
    const val = JSON.parse(rule.Child('quoted_str').value)
    let valRul = null
    switch (rule.Child('operator').value) {
      case '=':
        valRul = Sql.Eq(new Sql.Raw('service_name'), Sql.val(val))
        break
      case '!=':
        valRul = Sql.Ne(new Sql.Raw('service_name'), Sql.val(val))
        break
      case '=~':
        valRul = Sql.Eq(new Sql.Raw(`match(service_name, ${Sql.quoteVal(val)})`), 1)
        break
      case '!~':
        valRul = Sql.Ne(new Sql.Raw(`match(service_name, ${Sql.quoteVal(val)})`), 1)
    }
    conds = valRul
  }
  return conds || empty
}


const parseLabelSelector = (labelSelector) => {
  if (labelSelector.endsWith(',}')) {
    labelSelector = labelSelector.slice(0, -2) + '}'
  }
  return compiler.ParseScript(labelSelector).rootToken
}

/**
 *
 * @param typeId {string}
 */
const parseTypeId = (typeId) => {
  const typeParts = typeId.match(/^([^:]+):([^:]+):([^:]+):([^:]+):([^:]+)$/)
  if (!typeParts) {
    throw new QrynBadRequest('invalid type id')
  }
  return {
    type: typeParts[1],
    sampleType: typeParts[2],
    sampleUnit: typeParts[3],
    periodType: typeParts[4],
    periodUnit: typeParts[5]
  }
}


/**
 *
 * @param {Sql.Select} query
 * @param {string} labelSelector
 */
const labelSelectorQuery = (query, labelSelector) => {
  if (!labelSelector || !labelSelector.length || labelSelector === '{}') {
    return query
  }
  const labelSelectorScript = parseLabelSelector(labelSelector)
  const labelsConds = []
  for (const rule of labelSelectorScript.Children('log_stream_selector_rule')) {
    const val = JSON.parse(rule.Child('quoted_str').value)
    let valRul = null
    switch (rule.Child('operator').value) {
      case '=':
        valRul = Sql.Eq(new Sql.Raw('val'), Sql.val(val))
        break
      case '!=':
        valRul = Sql.Ne(new Sql.Raw('val'), Sql.val(val))
        break
      case '=~':
        valRul = Sql.Eq(new Sql.Raw(`match(val, ${Sql.quoteVal(val)})`), 1)
        break
      case '!~':
        valRul = Sql.Ne(new Sql.Raw(`match(val, ${Sql.quoteVal(val)})`), 1)
    }
    const labelSubCond = Sql.And(
      Sql.Eq('key', Sql.val(rule.Child('label').value)),
      valRul
    )
    labelsConds.push(labelSubCond)
  }
  query.where(Sql.Or(...labelsConds))
  query.groupBy(new Sql.Raw('fingerprint'))
  query.having(Sql.Eq(
    new Sql.Raw(`groupBitOr(${labelsConds.map((cond, i) => {
      return `bitShiftLeft(toUInt64(${cond}), ${i})`
    }).join('+')})`),
    new Sql.Raw(`bitShiftLeft(toUInt64(1), ${labelsConds.length})-1`)
  ))
}

const HISTORY_TIMESPAN = 1000 * 60 * 60 * 24 * 7

module.exports = {
  bufferize,
  parser,
  normalizeProtoResponse,
  wrapResponse,
  parseTypeId,
  serviceNameSelectorQuery,
  parseLabelSelector,
  labelSelectorQuery,
  HISTORY_TIMESPAN
}
