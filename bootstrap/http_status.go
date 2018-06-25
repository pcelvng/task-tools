package bootstrap

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"gopkg.in/jbsmith7741/uri.v0"
)

func (a *WorkerApp) handleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	// read the http request body
	body, _ := ioutil.ReadAll(r.Body)

	req := make(map[string]interface{})
	// if a body is provided and content type is application/json
	// unmarshal into the TaskRequest
	if len(body) > 0 &&
		strings.ToLower(r.Header.Get("Content-Type")) == "application/json" {

		err := json.Unmarshal(body, &req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"msg":"Error reading json request body","error":"%v"}`, err)
			return
		}
	}

	// if query params are provided, uri Unmarshal will override those values,
	// meaning query params take precedence
	if len(r.URL.Query()) > 0 {
		uri.Unmarshal(r.URL.String(), req)
	}

	fmt.Fprint(w, `{"response":"testing response"}`)
}
