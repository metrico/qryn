package tempo

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"strconv"
)

var tagsLexer = lexer.MustStateful(lexer.Rules{
	"Root": {
		{`OQuot`, `"`, lexer.Push("QString")},
		{`Literal`, `[^ !=~"]+`, nil},
		{`Cond`, `(!=|=~|!~|=)`, nil},
		{"space", `\s+`, nil},
	},
	"QString": {
		{"Escaped", `\\.`, nil},
		{"Char", `[^"]`, nil},
		{"CQuot", `"`, lexer.Pop()},
	},
})

type QuotedString struct {
	Str string
}

type LiteralOrQString struct {
	Literal string `@Literal`
	QString string `| (@OQuot(@Escaped|@Char)*@CQuot)`
}

func (l LiteralOrQString) Parse() (string, error) {
	if l.Literal != "" {
		return l.Literal, nil
	}
	return strconv.Unquote(l.QString)
}

type Tag struct {
	Name      LiteralOrQString `@@`
	Condition string           `@Cond`
	Val       LiteralOrQString `@@`
}

type Tags struct {
	Tags []Tag `@@*`
}

var tagsParser = participle.MustBuild[Tags](
	participle.Lexer(tagsLexer),
	participle.Elide("space"),
)
