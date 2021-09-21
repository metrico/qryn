const {_and, unquote_token, querySelectorPostProcess} = require("../common");

function selector_clauses(regex, eq, label, value) {
    return [
        `JSONHas(labels, '${label}')`,
        regex ? `extractAllGroups(JSONExtractString(labels, '${label}'), '(${value})') ${eq ? '!=' : '=='} []` :
            `JSONExtractString(labels, '${label}') ${eq ? '=' : '!='} '${value}'`
    ]
}

/**
 *
 * @param token {Token}
 * @returns {string[]}
 */
const label_and_val = (token) => {
    const label = token.Child('label').value;
    return [label, unquote_token(token)];
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.neq_simple = (token, query) => {
    const [label, value] = label_and_val(token);
    return querySelectorPostProcess(_and(query, selector_clauses(false, false, label, value)));
};

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.neq_extra_labels = (token, query) => {
    const [label, value] = label_and_val(token);

    return querySelectorPostProcess(_and(
        query,
        [['OR', `arrayExists(x -> x.1 == '${label}' AND x.2 != '${value}', extra_labels) != 0`,
            [
                'AND',
                `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                ...selector_clauses(false, false, label, value)
            ]
        ]]
    ));
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.neq_stream = (token, query) => {
    const [label, value] = label_and_val(token);
    return {
        ...query,
        stream: [...(query.stream ? query.stream : []),
             /**
              * @param stream {DataStream}
             */
             (stream) => stream.filter((e) =>
                e && e.labels && e.labels[label] && e.labels[label] !== value
            )
        ]
    };
};

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.nreg_simple = (token, query) => {
    const [label, value] = label_and_val(token);
    return querySelectorPostProcess(_and(query, selector_clauses(true, false, label, value)));
};

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.nreg_extra_labels = (token, query) => {
    const [label, value] = label_and_val(token);

    return querySelectorPostProcess(_and(
        query,
        [['OR', `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') == [], extra_labels) != 0`,
            [
                'AND',
                `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                ...selector_clauses(true, true, label, value)
            ]
        ]]
    ));
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.nreg_stream = (token, query) => {
    const [label, value] = label_and_val(token);
    const re = new RegExp(value);
    return {
        ...query,
        stream: [...(query.stream ? query.stream : []),
            /**
             * @param stream {DataStream}
             */
                (stream) => stream.filter((e) =>
                e && e.labels && e.labels[label] && !e.labels[label].match(re)
            )
        ]
    };
};

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.reg_simple = (token, query) => {
    const [label, value] = label_and_val(token);
    return querySelectorPostProcess(_and(query, selector_clauses(true, true, label, value)));
};

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.reg_extra_labels = (token, query) => {
    const [label, value] = label_and_val(token);

    return querySelectorPostProcess(_and(
        query,
        [['OR', `arrayExists(x -> x.1 == '${label}' AND extractAllGroups(x.2, '(${value})') != [], extra_labels) != 0`,
            [
                'AND',
                `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                ...selector_clauses(true, true, label, value)
            ]
        ]]
    ));
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.reg_stream = (token, query) => {
    const [label, value] = label_and_val(token);
    const re = new RegExp(value);
    return {
        ...query,
        stream: [...(query.stream ? query.stream : []),
            /**
             * @param stream {DataStream}
             */
             (stream) => stream.filter((e) =>
                e && e.labels && e.labels[label] && e.labels[label].match(re)
            )
        ]
    };
};

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq_simple = (token, query) => {
    const [label, value] = label_and_val(token);
    return querySelectorPostProcess(_and(query, selector_clauses(false, true, label, value)));
};
/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq_extra_labels = (token, query) => {
    const [label, value] = label_and_val(token);

    return querySelectorPostProcess(_and(
        query,
        [['OR', `indexOf(extra_labels, ('${label}', '${value}')) > 0`,
            [
                'AND',
                `arrayExists(x -> x.1 == '${label}', extra_labels) == 0`,
                ...selector_clauses(false, true, label, value)
            ]
        ]]
    ));
}

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @returns {registry_types.Request}
 */
module.exports.eq_stream = (token, query) => {
    const [label, value] = label_and_val(token);
    return {
        ...query,
        stream: [...(query.stream ? query.stream : []),
            /**
             * @param stream {DataStream}
             */
            (stream) => stream.filter((e) =>
                e && e.labels && e.labels[label] && e.labels[label] === value
            )
        ]
    };
};