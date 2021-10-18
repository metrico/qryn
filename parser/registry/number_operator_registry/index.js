const agg_reg = require("./compared_agg_reg");

/**
 *
 * @param token {Token}
 * @param query {registry_types.Request}
 * @param aggregated_processor {(function(Token, registry_types.Request): registry_types.Request)}
 * @returns {registry_types.Request}
 */
function generic_req(token, query, aggregated_processor) {
    if (token.name === 'compared_agg_statement' || token.Child('compared_agg_statement')) {
        return aggregated_processor(token, query);
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
        return generic_req(token, query, agg_reg.eq);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    ">": (token, query) => {
        return generic_req(token, query, agg_reg.gt);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    ">=": (token, query) => {
        return generic_req(token, query, agg_reg.ge);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "<": (token, query) => {
        return generic_req(token, query, agg_reg.lt);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "<=": (token, query) => {
        return generic_req(token, query, agg_reg.le);
    },

    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!=": (token, query) => {
        return generic_req(token, query, agg_reg.neq);
    }
};