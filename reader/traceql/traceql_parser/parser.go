package traceql_parser

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var parser = participle.MustBuild[TraceQLScript](
	participle.Lexer(TraceQLLexerDefinition), participle.UseLookahead(5))

// legacyOperators maps operators TraceQL does not have to its spelling.
var legacyOperators = map[string]string{"<<&": "&<<", "<<~": "!<<"}

func Parse(str string) (*TraceQLScript, error) {
	res, err := parser.ParseString("", str+" ")
	if err != nil {
		if legacy := legacyOperatorError(str); legacy != nil {
			return nil, legacy
		}
		return nil, err
	}
	return res, nil
}

// legacyOperatorError names the TraceQL spelling of a legacy operator in str.
func legacyOperatorError(str string) error {
	lex, err := TraceQLLexerDefinition.Lex("", strings.NewReader(str))
	if err != nil {
		return nil
	}
	tokens, err := lexer.ConsumeAll(lex)
	if err != nil {
		return nil
	}
	for _, tok := range tokens {
		if tempo, ok := legacyOperators[tok.Value]; ok {
			return fmt.Errorf("%s is not a TraceQL operator, use %s", tok.Value, tempo)
		}
	}
	return nil
}
