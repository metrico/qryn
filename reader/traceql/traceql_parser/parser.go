package traceql_parser

import (
	"github.com/alecthomas/participle/v2"
)

var parser = participle.MustBuild[TraceQLScript](
	participle.Lexer(TraceQLLexerDefinition), participle.UseLookahead(5))

func Parse(str string) (*TraceQLScript, error) {
	return parser.ParseString("", str+" ")
}
