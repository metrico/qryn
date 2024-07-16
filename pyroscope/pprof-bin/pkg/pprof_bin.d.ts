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
* @param {string} sample_type
*/
export function merge_tree(id: number, bytes: Uint8Array, sample_type: string): void;
/**
* @param {number} id
* @param {string} sample_type
* @returns {Uint8Array}
*/
export function export_tree(id: number, sample_type: string): Uint8Array;
/**
* @param {Uint8Array} payload
* @returns {Uint8Array}
*/
export function export_trees_pprof(payload: Uint8Array): Uint8Array;
/**
* @param {number} id
*/
export function drop_tree(id: number): void;
/**
*/
export function init_panic_hook(): void;
