const { bufferize } = require('./shared')

/**
 *
 * @param req
 */
const series = async (req, payload) => {
  let body = await bufferize(payload)
  body = JSON.parse(body.toString())
  req.type = 'json'
  return {
    getStart: () => body.start,
    getEnd: () => body.end,
    getMatchersList: () => body.matchers,
    getLabelNamesList: () => body.labelNames
  }
}

const getProfileStats = async (req, payload) => {
  req.type = 'json'
  return null
}

const settingsGet = async (req, payload) => {
  req.type = 'json'
  return {}
}

const labelNames = async (req, payload) => {
  req.type = 'json'
  let body = await bufferize(payload)
  body = JSON.parse(body.toString())
  return {
    getStart: () => body.start,
    getEnd: () => body.end,
    getName: () => body.name,
    getMatchersList: () => body.matchers
  }
}

const labelValues = async (req, payload) => {
  req.type = 'json'
  let body = await bufferize(payload)
  body = JSON.parse(body.toString())
  return {
    getName: () => body.name,
    getMatchersList: () => body.matchers,
    getStart: () => body.start,
    getEnd: () => body.end
  }
}

const analyzeQuery = async (req, payload) => {
  req.type = 'json'
  let body = await bufferize(payload)
  body = JSON.parse(body.toString())
  return {
    getStart: () => body.start,
    getEnd: () => body.end,
    getQuery: () => body.query
  }
}

module.exports = {
  series,
  getProfileStats,
  labelNames,
  labelValues,
  settingsGet,
  analyzeQuery
}
