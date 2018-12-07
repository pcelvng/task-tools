package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jbsmith7741/uri"

	"github.com/pcelvng/task"
	"github.com/pkg/errors"
)

type TaskRequest struct {
	TaskType     string `json:"task_type" uri:"task-type"`         // the task type for the batcher to use (should not be batcher)
	For          string `json:"for" uri:"for"`                     // go duration to create the tasks (used by batcher)
	From         hour   `json:"from" uri:"from"`                   // start time - format RFC 3339 YYYY-MM-DDTHH:MM:SSZ
	To           hour   `json:"to" uri:"to"`                       // end time - format RFC 3339 YYYY-MM-DDTHH:MM:SSZ
	DestTemplate string `json:"dest_template" uri:"fragment"`      // task destination template (uri fragment)
	Topic        string `json:"topic" uri:"topic"`                 // overrides task type as the default topic)
	EveryXHours  int    `json:"every_x_hours" uri:"every-x-hours"` // will generate a task every x hours. Includes the first hour. Can be combined with 'on-hours' and 'off-hours' options.)
	OnHours      []int  `json:"on_hours" uri:"on-hours"`           // comma separated list of hours to indicate which hours of a day to back-load during a 24 period (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. Example: '0,4,15' - will only generate tasks on hours 0, 4 and 15)
	OffHours     []int  `json:"off_hours" uri:"off-hours"`         // comma separated list of hours to indicate which hours of a day to NOT create a task (each value must be between 0-23). Order doesn't matter. Duplicates don't matter. If used will trump 'on-hours' values. Example: '2,9,16' - will generate tasks for all hours except 2, 9 and 16.)
	Template     string `json:"template" uri:"template"`
}

// handleBatch is a handler for /batch
func (opt *httpMaster) handleBatch(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	req := &TaskRequest{}
	if err := parseRequest(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"msg":"request could not be parsed","error":"%s"}`, err.Error())
		return
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
	// process template files if given
	if req.Template != "" {
		var found bool
		tsks := make([]*task.Task, 0)
		for _, v := range opt.Templates {
			if v.Name == req.Template {
				found = true
				req.DestTemplate = v.Info
				req.TaskType = v.Topic
				info := uri.Marshal(req)
				tsk := task.New(defaultTopic, info)
				opt.producer.Send(defaultTopic, tsk.JSONBytes())
				tsks = append(tsks, tsk)
			}
			if !found {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, `{"msg":"Error sending task","error":"template '%s' not found"}`, req.Template)
				return
			}
		}
		b, _ := json.Marshal(tsks)
		w.WriteHeader(http.StatusOK)
		w.Write(b)
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

func parseRequest(r *http.Request, i interface{}) error {
	// read the http request body
	body, _ := ioutil.ReadAll(r.Body)

	// there must be some kind of request body sent, or request query params
	if len(body) == 0 && len(r.URL.Query()) == 0 {
		return errors.New("missing request values")
	}

	// if a body is provided unmarshal into the TaskRequest
	if len(body) > 0 {
		err := json.Unmarshal(body, i)
		if err != nil {
			return errors.Wrapf(err, "body unmarshal")
		}
	}

	// if query params are provided, uri Unmarshal will override those values,
	// meaning query params take precedence
	if len(r.URL.Query()) > 0 {
		return uri.Unmarshal(r.URL.String(), i)
	}

	return nil
}

// returns an error if validation does not pass
func (tr TaskRequest) validate() error {
	if len(tr.TaskType) == 0 && tr.Template == "" {
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
			return fmt.Errorf("cannot parse for value %v - %v", tr.For, err)
		}
	}

	return nil
}

// handleStatus is a handler for the /status
func (opt *httpMaster) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")

	type params struct {
		App string `uri:"app" required:"true"`
	}

	a := &params{}
	if err := parseRequest(r, a); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"msg":"request could not be parsed","error":"%s"}`, err.Error())
		return
	}

	ip, found := opt.Apps[a.App]
	if !found {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"msg":"unknown app'%s'"}`, a.App)
		return
	}
	req, _ := http.NewRequest("GET", "http://"+ip, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(w, `{"msg":"%s}`, err)
		return
	}

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

// handleStats is a handler for the /stats
func (opt *httpMaster) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "text/plain")
	if opt.Stats == "" {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("stats not setup"))
		return
	}

	topics := make([]string, 0)
	for _, t := range r.URL.Query()["topic"] {
		var found bool
		for a := range opt.Apps {
			if strings.Contains(a, t) {
				found = true
				topics = append(topics, a)
			}
		}
		if !found {
			topics = append(topics, t)
		}
	}
	sort.Sort(sort.StringSlice(topics))
	url := struct {
		Scheme string   `uri:"scheme"`
		Host   string   `uri:"host"`
		Path   string   `uri:"path"`
		Topics []string `uri:"topic"`
	}{
		Scheme: "http",
		Path:   "stats",
		Host:   opt.Stats,
		Topics: topics,
	}

	req, err := http.NewRequest("GET", uri.Marshal(url), nil)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "invalid stats request: %s", err)
		return
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, err)
		return
	}
	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

type hour struct {
	time.Time
}

func (h *hour) UnmarshalJSON(b []byte) error {
	return h.UnmarshalText(b)
}
func (h *hour) UnmarshalText(b []byte) error {
	s := strings.Trim(string(b), `"`)
	t, err := time.Parse("2006-01-02T15", s)
	h.Time = t
	return err
}

func (h hour) MarshalText() ([]byte, error) {
	return []byte(h.String()), nil
}

func (h hour) String() string {
	return h.Format("2006-01-02T15")
}
