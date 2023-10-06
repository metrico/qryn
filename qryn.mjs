import {init, bun} from './qryn_node_wrapper.js'
import bunInit from './qryn_bun.mjs'

if (bun()) {
  bunInit()
} else {
  init()
}
