package promise

import (
	"sync"
	"testing"
)

func BenchmarkPromise(b *testing.B) {
	wg := sync.WaitGroup{}
	promises := make([]*Promise[int], b.N)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		promises[i] = New[int]()
		go func(p *Promise[int]) {
			defer wg.Done()
			p.Get()
		}(promises[i])
	}
	for _, p := range promises {
		p.Done(1, nil)
	}
	wg.Wait()
}
