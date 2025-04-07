//go:build !view
// +build !view

package view

import "embed"

var Static embed.FS

var HaveStatic bool = false
