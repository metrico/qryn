package plugins

var registry = make(map[string]any)

func registerPlugin[T any](name string) func(plugin T) {
	return func(plugin T) {
		registry[name] = plugin
	}
}

func getPlugin[T any](name string) func() *T {
	return func() *T {
		v, ok := registry[name]
		if !ok {
			return nil
		}
		_v, ok := v.(T)
		if !ok {
			return nil
		}
		return &_v
	}
}
