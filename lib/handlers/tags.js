/* Tags Label Handler */
/*
   For retrieving the names of the tags tempo can query on.
   Responses looks like this:
{
  "tagNames": [
    "instance",
    "job",
    ...
  ]
}
*/

function handler (req, res) {
  req.log.debug('GET /api/search/tags')
  const allLabels = this.labels.get('_LABELS_')
  const resp = { tagNames: allLabels }
  return res.send(resp)
};

module.exports = handler
