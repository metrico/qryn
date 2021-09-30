// Example Handler
function handler(req, res){
        if (this.debug) console.log("GET /api/v1/series/" + req.params.name + "/values");
        if (this.debug) console.log("QUERY SERIES: ", req.params);
        var resp = { status: "success", data: [] };
        res.send(resp);
};

module.exports = handler
