package logparser

import (
	"encoding/json"
	"fmt"
	"strings"
)

type DockerLogJson struct {
	Log string
}

type Decoder interface {
	Decode(string) (string, error)
}

type DockerJsonDecoder struct{}

func (d DockerJsonDecoder) Decode(src string) (string, error) {
	obj := DockerLogJson{}
	if err := json.Unmarshal([]byte(src), &obj); err != nil {
		return "", fmt.Errorf(`failed to unmarshal docker log entry "%s": %s`, src, err)
	}
	return obj.Log, nil
}

type CriDecoder struct{}

func (d CriDecoder) Decode(src string) (string, error) {
	c := 0
	i := strings.IndexFunc(src, func(r rune) bool {
		if r == ' ' {
			c++
		}
		return c == 3
	})
	if i < 0 {
		return "", fmt.Errorf("unexpected entry format: %s", src)
	}
	return src[i+1:], nil
}
