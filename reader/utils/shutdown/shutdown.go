package shutdown

func Shutdown(code int) {
	Chan <- code
}

var Chan = make(chan int)
