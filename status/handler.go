package status

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/fatih/structs"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus/info"
)

type statusFunc func() interface{}

type Handler struct {
	port      int
	genericFn []statusFunc
}

func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	data := h.Compile()
	b, _ := json.Marshal(data)
	/*if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		fmt.Fprintf(w, `{"err":"%s"}`, err)
	}*/
	w.Write(b)
}

func New(port int) *Handler {
	return &Handler{
		port:      port,
		genericFn: make([]statusFunc, 0),
	}
}

func (h *Handler) AddFunc(fn interface{}) error {
	var newFunc statusFunc
	switch statsFn := fn.(type) {
	case func() task.LauncherStats:
		newFunc = func() interface{} { return statsFn() }
	case func() info.Producer:
		newFunc = func() interface{} { return statsFn() }
	case func() info.Consumer:
		newFunc = func() interface{} { return statsFn() }
	case func() interface{}:
		newFunc = statsFn
	default:
		return fmt.Errorf("unsupported func type %T", statsFn)
	}
	h.genericFn = append(h.genericFn, newFunc)
	return nil
}

func (h *Handler) Compile() interface{} {
	data := make(map[string]interface{})
	for _, fn := range h.genericFn {
		v := fn()
		switch v.(type) {
		case task.LauncherStats:
			data["launcher"] = v
		case info.Consumer:
			data["consumer"] = v
		case info.Producer:
			data["producer"] = v
		default:
			m := structs.Map(v)
			for key, value := range m {
				data[key] = value
			}
		}
	}
	return data
}

func (h *Handler) Start() {
	log.Printf("starting http status server on port %d", h.port)

	http.HandleFunc("/", h.HandleRequest)
	go func() {
		err := http.ListenAndServe(":"+strconv.Itoa(h.port), nil)
		log.Fatal("http health service failed", err)
	}()

}
