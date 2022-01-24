const axios = require('axios')
/**
 *
 * @param id {string}
 * @param frequencySec {number}
 * @param startMs {number}
 * @param endMs {number}
 * @param extraLabels {Object}
 * @param msgGen? {(function(number): String)}
 * @param valGen? {(function(number): number)}
 * @param points {Object}
 */
module.exports.createPoints = (id, frequencySec,
  startMs, endMs,
  extraLabels, points, msgGen, valGen) => {
  const streams = {
    test_id: id,
    freq: frequencySec.toString(),
    ...extraLabels
  }
  msgGen = msgGen || ((i) => `FREQ_TEST_${i}`)
  const values = new Array(Math.floor((endMs - startMs) / frequencySec / 1000)).fill(0)
    .map((v, i) => valGen
      ? [((startMs + frequencySec * i * 1000) * 1000000).toString(), msgGen(i), valGen(i)]
      : [((startMs + frequencySec * i * 1000) * 1000000).toString(), msgGen(i)])
  points = { ...points }
  points[JSON.stringify(streams)] = {
    stream: streams,
    values: values
  }
  return points
}

const orgidHdr = (hdrs, orgid) => {
  if (!orgid) {
    return hdrs
  }
  return { ...hdrs, 'x-scope-orgid': orgid }
}

module.exports.orgidHdr = orgidHdr

/**
 *
 * @param points {Object<string, {stream: Object<string, string>, values: [string, string]}>}
 * @param endpoint {string}
 * @param orgid {string | undefined}
 * @returns {Promise<void>}
 */
module.exports.sendPoints = async (endpoint, points, orgid) => {
  try {
    console.log(`${endpoint}/loki/api/v1/push`)
    await axios.post(`${endpoint}/loki/api/v1/push`, {
      streams: Object.values(points)
    }, {
      headers: orgidHdr({ 'Content-Type': 'application/json' }, orgid)
    })
  } catch (e) {
    console.log(e.response)
    throw e
  }
}
