const stream_selector_operator_registry = require('./registry/stream_selector_operator_registry');
const line_filter_operator_registry = require('./registry/line_filter_operator_registry');
const {_and} = require("./registry/common");
const compiler = require("./bnf");
/**
 *
 * @returns {registry_types.Request}
 */
module.exports.init_query = () => {
    return {
        select: ['time_series.labels', 'samples.string', 'time_series.fingerprint', 'samples.timestamp_ms'],
        from: 'loki.samples',
        left_join: [{
            name: 'loki.time_series',
            on: ['AND', 'samples.fingerprint = time_series.fingerprint']
        }],
        limit: 1000,
        order_by: {
            name: 'timestamp_ms',
            order: 'desc'
        }
    };
}
const parseMs = (time, def) => {
    try {
        return time ? Math.floor(parseInt(time) / 1000000) : undefined;
    } catch (e) {
        return undefined;
    }
}
/**
 *
 * @param token {Token}
 * @param query {any}
 * @param fromMs {number | undefined}
 * @param toMs {number | undefined}
 * @returns {string}
 */
module.exports.transpile = (request) => {
    const expression = compiler.ParseScript(request.query);
    const token = expression.rootToken;
    let query = module.exports.init_query();
    if (request.limit) {
        query.limit = request.limit;
    }
    query.order_by.order = request.direction === 'forward' ? 'asc' : 'desc';
    for (const c of ['log_range_aggregation', 'aggregation_operator']) {
       if (token.Children(c).length > 0) {
           throw new Error(`${c} not supported`);
       }
    }
    query = module.exports.transpile_log_stream_selector(token, query);
    query = _and(query, [
        `timestamp_ms >= ${parseMs(request.start, Date.now() - 3600 * 1000)}`,
        `timestamp_ms <= ${parseMs(request.end, Date.now())}`
    ]);
    return module.exports.request_to_str(query);
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpile_log_stream_selector = (token, query) => {
    const rules = token.Children('log_stream_selector_rule');
    for(const rule of rules) {
        const op = rule.Child('operator').value;
        query = stream_selector_operator_registry[op](rule, query);
    }
    for(const pipeline of token.Children('line_filter_expression')) {
        const op = pipeline.Child('line_filter_operator').value;
        query = line_filter_operator_registry[op](pipeline, query);
    }
    for (const c of ['parser_expression','label_filter_expression','line_format_expression','labels_format_expression']) {
        if (token.Children(c).length > 0) {
            throw new Error(`${c} not supported`);
        }
    }
    return query;
}

/**
 *
 * @param query {registry_types.Request}
 * @returns {string}
 */
module.exports.request_to_str = (query) => {
    let req = `SELECT ${query.select.join(', ')} FROM ${query.from} `;
    for (const clause of query.left_join || []) {
        req += ` LEFT JOIN ${clause.name} ON ${whereBuilder(clause.on)}`;
    }
    req += query.where && query.where.length ? ` WHERE ${whereBuilder(query.where)} ` : '';
    req += query.order_by ? ` ORDER BY ${query.order_by.name} ${query.order_by.order} ` : '';
    req += typeof (query.limit) !== 'undefined' ? ` LIMIT ${query.limit}` : '';
    req += typeof (query.offset) !== 'undefined' ? ` OFFSET ${query.offset}` : '';
    return req;
}

/**
 *
 * @param clause {(string | string[])[]}
 */
const whereBuilder = (clause) => {
    const op = clause[0];
    let _clause = clause.slice(1).map(c => Array.isArray(c) ? `(${whereBuilder(c)})` : c);
    return _clause.join(` ${op} `);
}