const agg_reg = require("./compared_agg_reg");
const label_req = require("./compared_label_reg");

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param aggregated_processor {(function(Token, registry_types.Request): registry_types.Request)}
 * @param label_comparer {(function(Token, registry_types.Request): registry_types.Request)}
 * @returns {registry_types.Request}
 */
function generic_req(token, query,
                     aggregated_processor, label_comparer) {
    if (token.name === 'compared_agg_statement' || token.Child('compared_agg_statement')) {
        return aggregated_processor(token, query);
    }
    if (token.name === 'number_label_filter_expression' || token.Child('number_label_filter_expression')) {
        return label_comparer(token,query);
    }
    throw new Error('Not implemented');
}

module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "==": (token, query) => {
        return generic_req(token, query, agg_reg.eq, label_req.eq);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    ">": (token, query) => {
        return generic_req(token, query, agg_reg.gt, label_req.gt);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    ">=": (token, query) => {
        return generic_req(token, query, agg_reg.ge, label_req.ge);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "<": (token, query) => {
        return generic_req(token, query, agg_reg.lt, label_req.lt);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "<=": (token, query) => {
        return generic_req(token, query, agg_reg.le, label_req.le);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!=": (token, query) => {
        return generic_req(token, query, agg_reg.neq, label_req.neq);
    }
};