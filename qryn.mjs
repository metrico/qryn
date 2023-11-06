#!/usr/bin/env node

/*
 * qryn: polyglot observability API
 * (C) 2018-2024 QXIP BV
 */

import {init} from './qryn_node_wrapper.js'
import {bun} from './common.js'
import bunInit from './qryn_bun.mjs'

if (bun()) {
  bunInit()
} else {
  init()
}
