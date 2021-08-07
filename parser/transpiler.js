const stream_selector_operator_registry = require('./registry/stream_selector_operator_registry');
const line_filter_operator_registry = require('./registry/line_filter_operator_registry');
const log_range_aggregation_registry = require('./registry/log_range_aggregation_registry');
const high_level_aggregation_registry = require('./registry/high_level_aggregation_registry');
const {_and, durationToMs} = require("./registry/common");
const compiler = require("./bnf");
const {parseMs, DATABASE_NAME} = require("../lib/utils");


/**
 *
 * @returns {registry_types.Request}
 */
module.exports.init_query = () => {
    return {
        select: ['DISTINCT time_series.labels', 'samples.string', 'time_series.fingerprint as fingerprint',
            'samples.timestamp_ms as timestamp_ms'],
        from: `${DATABASE_NAME()}.samples`,
        left_join: [{
            name: `${DATABASE_NAME()}.time_series`,
            on: ['AND', 'samples.fingerprint = time_series.fingerprint']
        }],
        limit: 1000,
        order_by: {
            name: 'labels, timestamp_ms',
            order: 'desc'
        }
    };
}

/**
 *
 * @param request {{query: string, limit: number, direction: string, start: string, end: string, step: string}}
 * @returns {{query: string, matrix: boolean, duration: number | undefined}}
 */
module.exports.transpile = (request) => {
    const expression = compiler.ParseScript(request.query);
    const token = expression.rootToken;
    let start = parseMs(request.start, Date.now() - 3600 * 1000);
    let end = parseMs(request.end, Date.now());
    let step = request.step ? parseInt(request.step) * 1000 : 0;
    let query = module.exports.init_query();
    if (request.limit) {
        query.limit = request.limit;
    }
    query.order_by.order = request.direction === 'forward' ? 'asc' : 'desc';
    if (token.Child('aggregation_operator')) {
        const duration = durationToMs(token.Child('log_range_aggregation').Child('duration_value').value);
        start = Math.floor(start / duration) * duration;
        end = Math.ceil(end / duration) * duration;
        query.ctx = {
            start:start,
            end: end
        };
        query = _and(query, [
            `timestamp_ms >= ${start}`,
            `timestamp_ms <= ${end}`
        ]);
        query = module.exports.transpile_aggregation_operator(token, query);
    } else if (token.Child('log_range_aggregation')) {
        const duration = durationToMs(token.Child('log_range_aggregation').Child('duration_value').value);
        start = Math.floor(start / duration) * duration;
        end = Math.ceil(end / duration) * duration;
        query.ctx = {
            start:start,
            end: end,
            step: step
        };
        query = _and(query, [
            `timestamp_ms >= ${start}`,
            `timestamp_ms <= ${end}`
        ]);
        query = module.exports.transpile_log_range_aggregation(token, query);
    } else {
        query = module.exports.transpile_log_stream_selector(token, query);
        query = _and(query, [
            `timestamp_ms >= ${start}`,
            `timestamp_ms <= ${end}`
        ]);
    }
    return {
        query: module.exports.request_to_str(query),
        matrix: !! query.matrix,
        duration: query.ctx && query.ctx.duration ? query.ctx.duration : 1000
    };
}


/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpile_aggregation_operator = (token, query) => {
    const agg = token.Child("aggregation_operator");
    query = module.exports.transpile_log_range_aggregation(agg, query);
    return high_level_aggregation_registry[agg.Child("aggregation_operator_fn").value](token, query);
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.transpile_log_range_aggregation = (token, query) => {
    const agg = token.Child("log_range_aggregation");
    query = module.exports.transpile_log_stream_selector(agg, query);
    return log_range_aggregation_registry[agg.Child("log_range_aggregation_fn").value](token, query);
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
 * @param query {registry_types.Request | registry_types.UnionRequest}
 * @returns {string}
 */
module.exports.request_to_str = (query) => {
    if (query.requests) {
        return query.requests.map(r => `(${module.exports.request_to_str(r)})`).join(' UNION ALL ');
    }
    let req = query.with ? 'WITH ' + Object.entries(query.with).filter(e => e[1])
        .map(e => `${e[0]} as (${module.exports.request_to_str(e[1])})`).join(', ') :
        '';
    req += ` SELECT ${query.select.join(', ')} FROM ${query.from} `;
    for (const clause of query.left_join || []) {
        req += ` LEFT JOIN ${clause.name} ON ${whereBuilder(clause.on)}`;
    }
    req += query.where && query.where.length ? ` WHERE ${whereBuilder(query.where)} ` : '';
    req += query.group_by ? ` GROUP BY ${query.group_by.join(', ')}` : '';
    req += query.order_by ? ` ORDER BY ${query.order_by.name} ${query.order_by.order} ` : '';
    req += typeof (query.limit) !== 'undefined' ? ` LIMIT ${query.limit}` : '';
    req += typeof (query.offset) !== 'undefined' ? ` OFFSET ${query.offset}` : '';
    req += query.final ? ' FINAL' : '';
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