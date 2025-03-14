package unmarshal

import (
	"fmt"
	"github.com/go-faster/jx"
	"regexp"
	"strings"
	"testing"
)

const LEN = 64

func TestDDTags(t *testing.T) {
	var tagPattern = regexp.MustCompile(`([\p{L}][\p{L}_0-9\-.\\/]*):([\p{L}_0-9\-.\\/:]+)(,|$)`)
	for _, match := range tagPattern.FindAllStringSubmatch("env:staging,version:5.1,", -1) {
		println(match[1], match[2])
	}
}

func TestAppend(t *testing.T) {
	a := make([]string, 0, 10)
	b := append(a, "a")
	fmt.Println(b[0])
	a = a[:1]
	fmt.Println(a[0])
}

func BenchmarkFastAppend(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var res []byte
		res = append(res, fastFillArray(LEN, byte(1))...)
	}
}

func BenchmarkAppend(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var res []byte
		for c := 0; c < LEN; c++ {
			res = append(res, 1)
		}
	}
}

func BenchmarkAppendFill(b *testing.B) {
	a := make([]byte, 0, LEN)
	for i := 0; i < b.N; i++ {
		for c := 0; c < LEN; c++ {
			a = append(a, 5)
		}
	}
}

func TestJsonError(t *testing.T) {
	r := jx.Decode(strings.NewReader(`123`), 1024)
	fmt.Println(r.BigInt())
	//fmt.Println(r.Str())
}
