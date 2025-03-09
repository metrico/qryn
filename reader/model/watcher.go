package model

type QueryRangeOutput struct {
	Str string
	Err error
}

type IWatcher interface {
	Close()
	GetRes() chan QueryRangeOutput
	Done() <-chan struct{}
}
