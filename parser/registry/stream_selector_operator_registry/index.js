const reg = require('./stream_selector_operator_registry');
const {has_extra_labels} = require("../common");



module.exports = {
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!=": (token, query) => {
        if (query.stream) {
            return reg.eq_stream(token, query);
        }
        if (has_extra_labels(query)) {
            return  reg.neq_extra_labels(token, query);
        }
        return reg.neq_simple(token,query);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "=~": (token, query) => {
        if (query.stream) {
            return reg.reg_stream(token, query);
        }
        if (has_extra_labels(query)) {
            return  reg.reg_extra_labels(token, query);
        }
        return reg.reg_simple(token, query);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "!~": (token, query) => {
        if (query.stream) {
            return reg.nreg_stream(token, query);
        }
        if (has_extra_labels(query)) {
            return  reg.nreg_extra_labels(token, query);
        }
        return reg.nreg_simple(token, query);
    },
    /**
     *
     * @param token {Token}
     * @param query {registry_types.Request}
     * @returns {registry_types.Request}
     */
    "=": (token, query) => {
        if (query.stream) {
            return reg.eq_stream(token, query);
        }
        if (has_extra_labels(query)) {
            return  reg.eq_extra_labels(token, query);
        }
        return reg.eq_simple(token, query);
    }
};