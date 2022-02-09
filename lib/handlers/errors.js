class CLokiError extends Error {
  constructor (code, name, message) {
    super(message)
    this.code = code
    this.name = name
  }
}

class CLokiBadRequest extends CLokiError {
  constructor (message) {
    super(400, 'Bad Request', message)
  }
}

class CLokiNotFound extends CLokiError {
  constructor (message) {
    super(404, 'Not Found', message)
  }
}

const handler = (err, req, res) => {
  if (err instanceof CLokiError) {
    res.send({
      statusCode: err.code,
      error: err.name,
      message: err.message
    })
    return
  }
  /*if (res.raw.statusCode < 500) {

    // throw err
  }*/
  console.log({ err })
  res.send({
    statusCode: 500,
    error: 'Internal Server Error',
    message: 'Internal Server Error'
  })
}

module.exports = {
  CLokiError,
  CLokiBadRequest,
  CLokiNotFound,
  handler
}
