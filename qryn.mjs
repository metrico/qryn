import {init} from './qryn_node_wrapper.js'
import {bun} from './common.js'
import bunInit from './qryn_bun.mjs'

if (bun()) {
  bunInit()
} else {
  init()
}
