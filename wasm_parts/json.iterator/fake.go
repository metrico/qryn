package json_iterator

func RegisterTypeEncoderFunc(name string, f1 any, f2 any) {
}

type API struct {
}

func (api API) Marshal(v any) ([]byte, error) {
	return nil, nil
}

var ConfigCompatibleWithStandardLibrary = API{}
