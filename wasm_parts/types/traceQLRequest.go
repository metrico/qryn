package types

import (
	"wasm_parts/traceql/shared"
)

type TraceQLRequest struct {
	Request string
	Ctx     shared.PlannerContext
}
