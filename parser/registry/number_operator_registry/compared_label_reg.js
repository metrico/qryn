const {has_extra_labels, _and} = require("../common");
/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param stream_fn {(function(number, number): boolean)} stream_fn(current_val, searching_val)
 * @param extra_labels_search {(function (string, number): string)} extra_labels_search(label, searching_val)
 * @param labels_search {(function (string, number): string[])} labels_search(label, searching_val)
 * @returns {registry_types.Request}
 */
const generic_req = (token, query, stream_fn, extra_labels_search, labels_search) => {
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
        query.stream.push((s) => s.filter(e => {
            if (e.EOF) {
                return true;
            }
            if (!e || !e.labels || !e.labels[label]) {
                return false;
            }
            const _val = parseFloat(e.labels[label]);
            return !isNaN(_val) && stream_fn(_val, val)
        }));
        return query;
    }
    const _extra_labels_search = extra_labels_search(label, val);
    const _labels_search = labels_search(label, val);
    if (has_extra_labels(query)) {
        return _and(query, [
            //toFloat64OrNull(x.2) == ${val}

            //[`toFloat64OrNull(JSONExtractString(labels, '${label}')) == ${val}`]
            [ 'OR',
                `arrayExists(x -> x.1 == '${label}' AND (${_extra_labels_search}), extra_labels) != 0`,
                [
                    'AND',
                    `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                    `JSONHas(labels, '${label}')`,
                    ..._labels_search
                ]
            ]
        ]);
    }
    return _and(query, [
        `JSONHas(labels, '${label}')`,
        ..._labels_search
    ])
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param stream_fn {(function(number, number): boolean)} stream_fn(current_val, searching_val)
 * @param sign {string} just a sign in a generic expression
 * @returns {registry_types.Request}
 */
const operator_generic_req = (token, query, stream_fn, sign) =>
    generic_req(token, query,
        stream_fn, //
        (lbl, val) => `coalesce(toFloat64OrNull(x.2) ${sign} ${val}, 0)`,
        (label, val) => [`toFloat64OrNull(JSONExtractString(labels, '${label}')) ${sign} ${val}`]
    );

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq = (token, query) => {
    return operator_generic_req(token, query,
        (_val, val) => Math.abs(_val - val) < 1e-10, "==");
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.neq = (token, query) => {
    return operator_generic_req(token, query,
        (_val, val) => Math.abs(_val - val) > 1e-10,
        '!='
    );
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.gt = (token, query) => {
    return operator_generic_req(token, query,
        (_val, val) => _val > val,
        '>',
    );
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.ge = (token, query) => {
    return operator_generic_req(
        token, query,
        (_val, val) => _val >= val, '>='
    );
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.lt = (token, query) => {
    return operator_generic_req(
        token, query,
        (_val, val) => _val < val, '<'
    );
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.le = (token, query) => {
    return operator_generic_req(
        token, query,
        (_val, val) => _val <= val, '<='
    );
}
