package promise

import (
	"context"
	"fmt"
	"sync/atomic"
)

var GetContextTimeout = fmt.Errorf("get operation context timed out")

type Promise[T any] struct {
	lock    chan any
	err     error
	res     T
	pending int32
}

func New[T any]() *Promise[T] {
	res := &Promise[T]{
		lock:    make(chan any),
		pending: 1,
	}
	return res
}

func Fulfilled[T any](err error, res T) *Promise[T] {
	l := make(chan any)
	close(l)
	return &Promise[T]{
		lock:    l,
		err:     err,
		res:     res,
		pending: 0,
	}
}

func (p *Promise[T]) Get() (T, error) {
	<-p.lock
	return p.res, p.err
}

func (p *Promise[T]) GetCtx(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		var res T
		return res, GetContextTimeout
	case <-p.lock:
		return p.res, p.err
	}
}

func (p *Promise[T]) Done(res T, err error) {
	if !atomic.CompareAndSwapInt32(&p.pending, 1, 0) {
		return
	}
	p.res = res
	p.err = err
	close(p.lock)
}
