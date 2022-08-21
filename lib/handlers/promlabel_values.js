/* Label Value Handler */
/*
   For retrieving the label values one can query on.
   Responses looks like this:
  {
  "values": [
    "default",
    "cortex-ops",
    ...
  ]
}
*/

async function handler (req, res) {
  await require('./label_values.js')({
    ...req,
    query: {
      ...req.query,
      start: req.query.start ? parseInt(req.query.start) * 1e9 : undefined,
      end: req.query.end ? parseInt(req.query.end) * 1e9 : undefined
    }
  }, res)
}

module.exports = handler
