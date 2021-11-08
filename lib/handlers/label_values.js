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

function handler (req, res) {
  if (this.debug) { console.log('GET /api/prom/label/' + req.params.name + '/values') }
  if (this.debug) console.log('QUERY LABEL: ', req.params.name)
  const all_values = this.labels.get(req.params.name)
  const resp = { status: 'success', data: all_values }
  res.send(resp)
};

module.exports = handler
