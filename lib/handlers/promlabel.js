/* Label Handler */
/*
   For retrieving the names of the labels one can query on.
   Responses looks like this:
{
  "values": [
    "instance",
    "job",
    ...
  ]
}
*/

async function handler (req, res) {
  await require('./label.js')({
    ...req,
    query: {
      ...req.query,
      start: req.query.start ? parseInt(req.query.start) * 1e9 : undefined,
      end: req.query.end ? parseInt(req.query.end) * 1e9 : undefined
    }
  }, res)
}

module.exports = handler
