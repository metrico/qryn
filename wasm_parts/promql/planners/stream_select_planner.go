package planners

import (
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"strings"
	"wasm_parts/promql/shared"
	sql "wasm_parts/sql_select"
)

type StreamSelectPlanner struct {
	Main     shared.RequestPlanner
	Matchers []*labels.Matcher
}

func (s *StreamSelectPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := s.Main.Process(ctx)
	if err != nil {
		return nil, err
	}
	conds := make([]sql.SQLCondition, len(s.Matchers))
	for i, m := range s.Matchers {
		conds[i], err = s.getCond(m)
		if err != nil {
			return nil, err
		}
	}
	main.AndWhere(sql.Or(conds...))

	bitSetEntries := make([]*bitSetEntry, len(conds))
	for i, c := range conds {
		bitSetEntries[i] = &bitSetEntry{c, i}
	}
	main.AndHaving(sql.Eq(&bitSet{entries: bitSetEntries}, sql.NewIntVal((int64(1)<<uint(len(conds)))-1)))
	return main, nil
}

func (s *StreamSelectPlanner) getCond(m *labels.Matcher) (sql.SQLCondition, error) {
	keyCond := sql.Eq(sql.NewRawObject("key"), sql.NewStringVal(m.Name))
	var valCond sql.SQLCondition
	switch m.Type {
	case labels.MatchEqual:
		valCond = sql.Eq(sql.NewRawObject("val"), sql.NewStringVal(m.Value))
	case labels.MatchNotEqual:
		valCond = sql.Neq(sql.NewRawObject("val"), sql.NewStringVal(m.Value))
	case labels.MatchRegexp:
		valCond = sql.Eq(&pregMatch{sql.NewRawObject("val"), sql.NewStringVal(m.Value)},
			sql.NewIntVal(1))
	case labels.MatchNotRegexp:
		valCond = sql.Eq(&pregMatch{sql.NewRawObject("val"), sql.NewStringVal(m.Value)},
			sql.NewIntVal(0))
	default:
		return nil, fmt.Errorf("unknown matcher type: %v", m.Type)
	}
	return sql.And(keyCond, valCond), nil
}

type pregMatch struct {
	key sql.SQLObject
	val sql.SQLObject
}

func (p *pregMatch) String(ctx *sql.Ctx, options ...int) (string, error) {
	strK, err := p.key.String(ctx, options...)
	if err != nil {
		return "", err
	}
	strV, err := p.val.String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("match(%s, %s)", strK, strV), nil
}

type bitSetEntry struct {
	cond sql.SQLCondition
	idx  int
}

func (b bitSetEntry) String(ctx *sql.Ctx, options ...int) (string, error) {
	strCond, err := b.cond.String(ctx, options...)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("bitShiftLeft(toUInt64(%s), %d)", strCond, b.idx), nil
}

type bitSet struct {
	entries []*bitSetEntry
}

func (b bitSet) String(ctx *sql.Ctx, options ...int) (string, error) {
	strEntries := make([]string, len(b.entries))
	var err error
	for i, e := range b.entries {
		strEntries[i], err = e.String(ctx, options...)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("groupBitOr(%s)", strings.Join(strEntries, "+")), nil
}
