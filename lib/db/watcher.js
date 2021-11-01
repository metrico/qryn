const transpiler = require("../../parser/transpiler");
const EventEmitter = require("events");
const UTILS = require("../utils");
const {queryFingerprintsScan} = require("./clickhouse");

module.exports = class extends EventEmitter {
    constructor(request) {
        super();
        this.request = request;

        this.step = UTILS.parseOrDefault(request.step, 5) * 1000;
        const self = this;
        this.working = true;
        this.initQuery().catch(e => self.emit('error', e));
    }

    initQuery() {
        return this.initQueryCBPoll();
    }

    async initQueryCBPoll() {
        this.from = (Date.now() - 300000) * 1000000;
        while (this.working) {
            this.to = (Date.now() - 5000) * 1000000;
            this.query = transpiler.transpile({
                ...this.request,
                start: this.from,
                end: this.to
            });
            this.query.step = this.step;
            await queryFingerprintsScan(this.query, {
                res: this
            });
            this.from = this.to;
            await new Promise(f => setTimeout(f, 1000));
        }
    }

    writeHead() {}
    write (str) {

        this.resp = this.resp || "";
        this.resp += str;

    }
    end() {
        this.emit('data', JSON.stringify(
            {
                streams: JSON.parse(this.resp).data.result
            }
        ));
        this.resp = "";
    }

    destroy() {
        this.working = false;
    }
}