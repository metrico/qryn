package controllerv1

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/metrico/qryn/reader/utils/logger"
	watchdog "github.com/metrico/qryn/reader/watchdog"
	"net/http"
)

type MiscController struct {
	Version string
}

func (uc *MiscController) Ready(w http.ResponseWriter, r *http.Request) {
	err := watchdog.Check()
	if err != nil {
		w.WriteHeader(500)
		logger.Error(err.Error())
		w.Write([]byte("Internal Server Error"))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func (uc *MiscController) Config(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Not supported"))
}

func (uc *MiscController) Rules(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"data": {"groups": []},"status": "success"}`))
}

func (uc *MiscController) Metadata(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "success","data": {}}`))
}

func (uc *MiscController) Buildinfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	//w.Write([]byte(fmt.Sprintf(`{"status": "success","data": {"version": "%s"}}`, uc.Version)))
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	defer jsoniter.ConfigFastest.ReturnStream(stream)

	// Build the JSON response:
	// {"status": "success","data": {"version": "<uc.Version>"}}
	stream.WriteRaw(`{"status": "success","data": {"version": `)
	stream.WriteString(uc.Version) // WriteString will output a properly quoted JSON string.
	stream.WriteRaw(`}}`)

	w.Write([]byte(string(stream.Buffer())))
}
