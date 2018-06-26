package status

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus/info"
)

type Handler struct {
	genericFn []func() interface{}
}

func (a *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	data := a.Compile()
	b, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		fmt.Fprintf(w, `{"err":"%s"}`, err)
	}
	w.Write(b)
}

func New() *Handler {
	return nil
}

func (h *Handler) AddFunc(fn interface{}) {
	var newFunc func() interface{}
	switch v := fn.(type) {
	case func() task.LauncherStats:
		newFunc = func() interface{} { return v }
	case func() info.Producer:
		newFunc = func() interface{} { return v }
	case func() info.Consumer:
		newFunc = func() interface{} { return v }
	case func() interface{}:
		newFunc = v
	default:
		log.Fatal("Unsupported func type %T", v)
	}
	h.genericFn = append(h.genericFn, newFunc)
}

func (h *Handler) Compile() interface{} {
	data := make([]interface{}, len(h.genericFn))
	for i, fn := range h.genericFn {
		data[i] = fn()
	}
	return data
}
