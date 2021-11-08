const {scanSeries} = require("../db/clickhouse");

// Example Handler
async function handler(req, res){
        try {
                if (!req.query.match) {
                        throw new Error('Match param is required');
                }
                await scanSeries(Array.isArray(req.query.match) ? req.query.match : [req.query.match],
                    {res: res.raw});
        } catch (e) {
                throw e;
        }
};

module.exports = handler
