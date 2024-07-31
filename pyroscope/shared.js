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

module.exports = {
  bufferize
}
