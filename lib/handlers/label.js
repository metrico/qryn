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

function handler(req, res){
        if (this.debug) console.log("GET /loki/api/v1/label");
        if (this.debug) console.log("QUERY: ", req.query);
        var all_labels = this.labels.get("_LABELS_");
        var resp = { values: all_labels };
        res.send(resp);
};

module.exports = handler
