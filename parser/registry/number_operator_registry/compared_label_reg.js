const { has_extra_labels, _and } = require('../common')
/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param index {string}
 * @returns {registry_types.Request}
 */
const generic_req = (token, query, index) => {
    if (token.Child('number_value').Child('duration_value') ||
        token.Child('number_value').Child('bytes_value')) {
        throw new Error('Not implemented');
    }
    const label = token.Child('label').value;
    const val = parseInt(token.Child('number_value').value);
    if (isNaN(val)) {
        throw new Error(token.Child('number_value').value + 'is not a number');
    }
    if (query.stream && query.stream.length) {
        return {
            ...query,
            stream: [...(query.stream || []),
                /**
                 *
                 * @param s {DataStream}
                 */
                (s) => s.filter(module.exports.stream_where[index](label, val))
            ],

        };
    }
    if (has_extra_labels(query)) {
        return _and(query, [module.exports.extra_labels_where[index](label, val)]);
    }
    return _and(query, module.exports.simple_where[index](label, val));
}

/**
 *
 * @param label {string}
 * @param val {string}
 * @param sign {string}
 * @returns {[string]}
 */
const generic_simple_label_search =
    (label, val, sign) => [
        'and',
        `JSONHas(labels, '${label}')`
        `toFloat64OrNull(JSONExtractString(labels, '${label}')) ${sign} ${val}`];

/**
 *
 * @param lbl {string}
 * @param val {string}
 * @param sign {string}
 * @returns {[string]}
 */
const generic_extra_label_search =
    (lbl, val, sign) => [ 'or',
        `arrayExists(x -> x.1 == '${lbl}' AND (coalesce(toFloat64OrNull(x.2) ${sign} ${val}, 0)), extra_labels) != 0`,
        [
            'AND',
            `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
            ...(generic_simple_label_search(lbl, val, sign).slice(1))
        ]
    ];

const generic_stream_search = (label, fn) => (
    (e) => {
        if (e.EOF) {
            return true;
        }
        if (!e || !e.labels || !e.labels[label]) {
            return true;
        }
        const val = parseFloat(e.labels[label]);
        if (isNaN(val)) {
            return false;
        }
        return fn(val);
    }
)

module.exports.simple_where = {
    eq: (label, val) => generic_simple_label_search(label, val, '=='),
    neq: (label, val) => generic_simple_label_search(label, val, '!='),
    ge: (label, val) => generic_simple_label_search(label, val, '>='),
    gt: (label, val) => generic_simple_label_search(label, val, '>'),
    le: (label, val) => generic_simple_label_search(label, val, '<='),
    lt: (label, val) => generic_simple_label_search(label, val, '<')
}

module.exports.extra_labels_where = {
    eq: (label, val) => generic_extra_label_search(label, val, '=='),
    neq: (label, val) => generic_extra_label_search(label, val, '!='),
    ge: (label, val) => generic_extra_label_search(label, val, '>='),
    gt: (label, val) => generic_extra_label_search(label, val, '>'),
    le: (label, val) => generic_extra_label_search(label, val, '<='),
    lt: (label, val) => generic_extra_label_search(label, val, '<')
}

module.exports.stream_where = {
    eq: (label, val) => generic_stream_search((_val) => Math.abs(val - _val) < 1e-10 ),
    neq: (label, val) => generic_stream_search((_val) => Math.abs(val - _val) > 1e-10 ),
    ge: (label, val) => generic_stream_search((_val) => val >= _val),
    gt: (label, val) => generic_stream_search((_val) => val > _val),
    le: (label, val) => generic_stream_search((_val) => val <= _val),
    lt: (label, val) => generic_stream_search((_val) => val < _val),
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq = (token, query) => {
    return generic_req(token, query, 'eq');
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.neq = (token, query) => {
    return generic_req(token, query, 'neq');
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.gt = (token, query) => {
    return generic_req(token, query, 'gt');
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.ge = (token, query) => {
    return generic_req(token, query, 'ge');
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.lt = (token, query) => {
    return generic_req(token, query, 'lt');
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.le = (token, query) => {
    return generic_req(token, query, 'le');
}
