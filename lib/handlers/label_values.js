// Example Handler
function handler(req, res){
        if (this.debug)
                console.log("GET /api/prom/label/" + req.params.name + "/values");
        if (this.debug) console.log("QUERY LABEL: ", req.params.name);
        var all_values = this.labels.get(req.params.name);
        var resp = { values: all_values };
        res.send(resp);
};

module.exports = handler
