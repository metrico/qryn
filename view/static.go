//go:build view
// +build view

package view

import "embed"

//go:embed dist
var Static embed.FS

var HaveStatic bool = true
