package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pcelvng/task"
	"gopkg.in/jbsmith7741/uri.v0"
)

type TaskRequest struct {
	TaskType    string    `json:"task_type" uri:"task-type"`         // the task type for the batcher to use (should not be batcher)
	For         string    `json:"for" uri:"for"`                     // go duration to create the tasks (used by batcher)
	From        time.Time `json:"from" uri:"from"`                   // start time - format RFC 3339 YYYY-MM-DDTHH:MM:SSZ
	To          time.Time `json:"to" uri:"to"`                       // end time - format RFC 3339 YYYY-MM-DDTHH:MM:SSZ
	Fragment    string    `json:"fragment" uri:"fragment"`           // task destination template (uri fragment)
	Topic       string    `json:"topic" uri:"topic"`                 // overrides task type as the default topic)
	EveryXHours int       `json:"every_x_hours" uri:"every-x-hours"` // will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' options.)
	OnHours     []int     `json:"on_hours" uri:"on-hours"`           // comma separated list of hours to indicate which hours of a day to back-load during a 24 period (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. Example: '0,4,15' - will only generate tasks on hours 0, 4 and 15)
	OffHours    []int     `json:"off_hours" uri:"off-hours"`         // comma separated list of hours to indicate which hours of a day to NOT create a task (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. If used will trump 'on-hours' values. Example: '2,9,16' - will generate tasks for all hours except 2, 9 and 16.)
}

func (opt *options) handleRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	// read the http request body
	body, _ := ioutil.ReadAll(r.Body)

	// there must be some kind of request body sent, or request query params
	if len(body) == 0 && len(r.URL.Query()) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"msg":"missing request values"}`)
		return
	}

	req := new(TaskRequest)
	// if a body is provided unmarshal into the TaskRequest
	if len(body) > 0 {
		err := json.Unmarshal(body, req)
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

	// if 'for' and 'to' are not provided, run for only one time
	if req.For == "" && req.To.IsZero() {
		req.To = req.From
	}

	err := req.validate()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"msg":"request validation issue","error":"%v"}`, err)
		return
	}

	info := uri.Marshal(req)
	tskJson := task.New(defaultTopic, info).JSONBytes()
	err = opt.producer.Send(defaultTopic, tskJson)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"msg":"Error sending task","error":"%v"}`, err)
		return
	}

	fmt.Fprint(w, string(tskJson))
}

// returns an error if validation does not pass
func (tr TaskRequest) validate() error {
	if len(tr.TaskType) == 0 {
		return fmt.Errorf("task type is required")
	}

	if tr.From.IsZero() {
		return fmt.Errorf("from value is required")
	}

	if tr.To.IsZero() && len(tr.For) == 0 {
		return fmt.Errorf("to value is required if for value is not provided")
	}

	if len(tr.For) > 0 {
		_, err := time.ParseDuration(tr.For)
		if err != nil {
			return fmt.Errorf("cannot parse for value %v", err)
		}
	}

	return nil
}
