package shared

type NotSupportedError struct {
	Msg string
}

func (n *NotSupportedError) Error() string {
	return n.Msg
}

func isNotSupportedError(e error) bool {
	_, ok := e.(*NotSupportedError)
	return ok
}
