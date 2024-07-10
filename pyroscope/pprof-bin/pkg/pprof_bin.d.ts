/* tslint:disable */
/* eslint-disable */
/**
* @param {number} id
* @param {Uint8Array} bytes
* @param {string} sample_type
*/
export function merge_prof(id: number, bytes: Uint8Array, sample_type: string): void;
/**
* @param {number} id
* @param {Uint8Array} bytes
*/
export function merge_tree(id: number, bytes: Uint8Array): void;
/**
* @param {number} id
* @returns {Uint8Array}
*/
export function export_tree(id: number): Uint8Array;
/**
* @param {Uint32Array} ids
* @param {string} period_type
* @param {string} period_unit
* @param {string} _sample_types
* @param {string} _sample_units
* @returns {Uint8Array}
*/
export function export_trees_pprof(ids: Uint32Array, period_type: string, period_unit: string, _sample_types: string, _sample_units: string): Uint8Array;
/**
* @param {number} id
*/
export function drop_tree(id: number): void;
/**
*/
export function init_panic_hook(): void;
