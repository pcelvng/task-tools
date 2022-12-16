package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pcelvng/task/bus/nop"

	"github.com/hydronica/trial"
	"github.com/jarcoal/httpmock"
	"github.com/pcelvng/task/bus"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// disable app logging
	log.SetOutput(ioutil.Discard)

	os.Exit(m.Run())
}

// if no request values are provided, returns a 400
// response should always be Content-Type of application/json
func TestHandleNoRequestValues(t *testing.T) {
	req := httptest.NewRequest("GET", "localhost:8080", nil)
	w := httptest.NewRecorder()

	opts := newOptions()
	opts.handleBatch(w, req)

	resp := w.Result()
	//body, _ := ioutil.ReadAll(resp.Body)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

// if request values are passed (as json in the body of the request)
// response should send to the selected bus
// response should always be Content-Type of application/json
func TestHandleBodyRequestValues(t *testing.T) {
	reader := strings.NewReader(`{"task_type":"fb-hourly-loader","from":"2018-05-01T00"}`)
	req := httptest.NewRequest("POST", "localhost:8080", reader) // could be any http method that has a body ie: GET, PUT, DELETE
	w := httptest.NewRecorder()
	opts := newOptions()

	if opts.Bus.Bus != "-" {
		opts.producer, _ = bus.NewProducer(opts.Bus)
	}

	opts.handleBatch(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	urlUnescapeBody, _ := url.QueryUnescape(string(body))

	assert.Contains(t, urlUnescapeBody, `{"type":"batcher","info":"?from=2018-05-01T00&task-type=fb-hourly-loader&to=2018-05-01T00"`)
}

func TestQueryParamsRequest(t *testing.T) {
	req := httptest.NewRequest("GET", "localhost:8080?task-type=fee-campaign&from=2018-05-01T00", nil) // could be any http method that has a body ie: GET, PUT, DELETE
	w := httptest.NewRecorder()
	opts := newOptions()

	opts.producer, _ = bus.NewProducer(opts.Bus)

	opts.handleBatch(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `{"type":"batcher"`)
}

func TestValidationRequestError(t *testing.T) {
	req := httptest.NewRequest("GET", "localhost:8080?task-type=fee-campaign", nil) // could be any http method that has a body ie: GET, PUT, DELETE
	w := httptest.NewRecorder()
	opts := newOptions()

	opts.handleBatch(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "error")
}

func TestHandleBadBodyRequest(t *testing.T) {
	reader := strings.NewReader(`{`)
	req := httptest.NewRequest("POST", "localhost:8080", reader) // could be any http method that has a body ie: GET, PUT, DELETE
	w := httptest.NewRecorder()
	opts := newOptions()

	opts.handleBatch(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	assert.Contains(t, string(body), "error")
}

func TestHandleBadSendTask(t *testing.T) {
	req := httptest.NewRequest("GET", "localhost:8080?task-type=fee-campaign&from=2018-05-01T00", nil)
	w := httptest.NewRecorder()

	opts := newOptions()
	opts.Bus.NopMock = "send_err"
	opts.producer, _ = bus.NewProducer(opts.Bus)

	opts.handleBatch(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	assert.Contains(t, string(body), "error")
}

func TestValidate(t *testing.T) {
	req := &TaskRequest{}

	err := req.validate()
	assert.NotNil(t, err) // task type is required

	req.TaskType = "a value"
	err = req.validate()
	assert.NotNil(t, err) // from is required

	req.From = hour{time.Now()}
	err = req.validate()
	assert.NotNil(t, err) // to value required if for value is not provided

	req.To = hour{time.Now()}
	err = req.validate()
	assert.Nil(t, err)

	req.For = "blahblah"
	err = req.validate()
	assert.NotNil(t, err) // bad for value
}

func TestHandleStatus(t *testing.T) {
	// test setup
	hm := &httpMaster{
		Apps: map[string]string{
			"valid":   "endpoint:10000",
			"timeout": "null",
		},
	}

	// mock status endpoints for the two dummy apps valid and timeout
	httpmock.Activate()
	defer httpmock.Deactivate()
	httpmock.RegisterResponder("GET", "http://endpoint:10000",
		httpmock.NewStringResponder(http.StatusOK, `{"msg":"app ok"}`))
	httpmock.RegisterResponder("GET", "http://null",
		httpmock.NewErrorResponder(errors.New("connection refused")))

	fn := func(req *http.Request) (string, error) {
		w := httptest.NewRecorder()
		hm.handleStatus(w, req)

		if w.Code != http.StatusOK {
			return "", errors.New(w.Body.String())
		}

		if err := json.Unmarshal(w.Body.Bytes(), &struct{}{}); err != nil {
			return "", fmt.Errorf("invalid json response %q", err)
		}
		return w.Body.String(), nil
	}
	cases := trial.Cases[*http.Request, string]{
		"successful call": {
			Input:    httptest.NewRequest("GET", "http://path/status?app=valid", nil),
			Expected: `{"msg":"app ok"}`,
		},
		"no response": {
			Input:       httptest.NewRequest("GET", "http://path/status?app=timeout", nil),
			ExpectedErr: errors.New("connection refused"),
		},
		"missing params": {
			Input:       httptest.NewRequest("GET", "http://", nil),
			ExpectedErr: errors.New("missing request values"),
		},

		"non-registered app": {
			Input:       httptest.NewRequest("GET", "http://path/status?app=missing", nil),
			ExpectedErr: errors.New("unknown app"),
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestHandleBatch(t *testing.T) {
	hm := &httpMaster{
		Templates: []template{
			{Name: "group1", Info: "s3://path/to/file.gz?hour=2018-01-01T00", Topic: "task1"},
			{Name: "group1", Info: "s3://path/to/file.gz?hour=2018-01-01T00", Topic: "task2"},
		},
	}
	fn := func(req *http.Request) ([]string, error) {
		w := httptest.NewRecorder()
		p, _ := nop.NewProducer("")
		hm.producer = p
		hm.handleBatch(w, req)
		if w.Code != http.StatusOK {
			return nil, errors.New(w.Body.String())
		}
		type info struct {
			Info string `json:"info"`
		}
		tsks := make([]string, len(p.Messages["batcher"]))
		for i, v := range p.Messages["batcher"] {
			in := &info{}
			json.Unmarshal([]byte(v), in)
			tsks[i] = in.Info
		}
		return tsks, nil
	}
	cases := trial.Cases[*http.Request, []string]{
		"simple batch": {
			Input:    httptest.NewRequest("GET", "http://localhost:8080/batch?task-type=task&from=2018-01-01T00#?s3://data.json.gz", nil),
			Expected: []string{`?from=2018-01-01T00&task-type=task&to=2018-01-01T00#?s3://data.json.gz`},
		},
		"group1": {
			Input: httptest.NewRequest("GET", "http://localhost:8080/batch?template=group1&from=2018-01-01T00", nil),
			Expected: []string{
				`from=2018-01-01T00&task-type=task1&template=group1&to=2018-01-01T00#s3://path/to/file.gz?hour=2018-01-01T00`,
				`from=2018-01-01T00&task-type=task2&template=group1&to=2018-01-01T00#s3://path/to/file.gz?hour=2018-01-01T00`,
			},
		},
		"template not found": {
			Input:       httptest.NewRequest("GET", "http://localhost:8080?template=invalid&from=2018-01-01T00", nil),
			ExpectedErr: errors.New("template 'invalid' not found"),
		},
		"invalid time format": {
			Input:       httptest.NewRequest("GET", "http://localhost?template=group1&from=2018-111-11", nil),
			ExpectedErr: errors.New("request could not be parsed"),
		},
	}

	trial.New(fn, cases).EqualFn(trial.Contains).Test(t)
}

func TestHandleStats(t *testing.T) {
	type input struct {
		master  httpMaster
		request string
	}

	// setup the mock responder to response with the request.
	// this lets us verify that the request being made is as expected
	httpmock.Activate()
	defer httpmock.Deactivate()
	httpmock.RegisterResponder("GET", "http://endpoint:100/stats", echoURLResponse)

	fn := func(in input) (string, error) {
		w := httptest.NewRecorder()

		req := httptest.NewRequest("GET", in.request, nil)
		(&in.master).handleStats(w, req)
		if w.Code != http.StatusOK {
			return "", errors.New(w.Body.String())
		}
		return w.Body.String(), nil
	}
	cases := trial.Cases[input, string]{
		"no stats setup": {
			Input:       input{request: "/stats"},
			ExpectedErr: errors.New("stats not setup"),
		},
		"request all topics": {
			Input: input{httpMaster{Stats: "endpoint:100"}, "/stats?"},
		},
		"request 2 topics": {
			Input: input{
				master:  httpMaster{Stats: "endpoint:100"},
				request: "/stats?topic=task1,task2",
			},
			Expected: "http://endpoint:100/stats?topic=task1&topic=task2",
		},
		"name matching": {
			Input: input{
				master: httpMaster{
					Stats: "endpoint:100",
					Apps: map[string]string{
						"task1": "",
						"task2": "",
						"task3": "",
					}},
				request: "/stats?topic=task",
			},
			Expected: "http://endpoint:100/stats?topic=task1&topic=task2&topic=task3",
		},
		"error from stats request": {
			Input: input{master: httpMaster{Stats: "invalid"},
				request: "/stats",
			},
			ShouldErr: true,
		},
		"invalid stats endpoint": {
			Input: input{master: httpMaster{Stats: "/"},
				request: "/stats",
			},
			ExpectedErr: errors.New("invalid stats request"),
		},
	}

	trial.New(fn, cases).EqualFn(trial.Contains).Test(t)
}

// echoURLResponse returns the url in the body of the response
func echoURLResponse(req *http.Request) (*http.Response, error) {
	body := httpmock.NewRespBodyFromString(req.URL.String())
	return &http.Response{Request: req, StatusCode: http.StatusOK, Body: body}, nil
}
