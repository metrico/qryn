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

const labelNames = async (req, payload) => {
  req.type = 'json'
  let body = await bufferize(payload)
  body = JSON.parse(body.toString())
  return {
    getStart: () => body.start,
    getEnd: () => body.end,
    getName: () => body.name
  }
}

module.exports = {
  series,
  getProfileStats,
  labelNames
}
