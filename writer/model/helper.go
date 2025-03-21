package model

type StrStr struct {
	Str1 string
	Str2 string
}

type ValuesAgg struct {
	ValueStr   string
	ValueInt64 int64
	ValueInt32 int32
}
type ValuesArrTuple struct {
	ValueStr         string
	FirstValueInt64  int64
	SecondValueInt64 int64
}
type TreeRootStructure struct {
	Field1        uint64
	Field2        uint64
	Field3        uint64
	ValueArrTuple []ValuesArrTuple
}

type Function struct {
	ValueInt64 uint64
	ValueStr   string
}
