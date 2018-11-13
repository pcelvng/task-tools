package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jbsmith7741/trial"
	"github.com/pcelvng/task/bus"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/jarcoal/httpmock.v1"
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
	reader := strings.NewReader(`{"task_type":"fb-hourly-loader","from":"2018-05-01T00:00:00Z"}`)
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

	assert.Contains(t, urlUnescapeBody, `{"type":"batcher","info":"?from=2018-05-01T00:00:00Z\u0026task-type=fb-hourly-loader\u0026to=2018-05-01T00:00:00Z"`)
}

func TestQueryParamsReqeust(t *testing.T) {
	req := httptest.NewRequest("GET", "localhost:8080?task-type=fee-campaign&from=2018-05-01T00:00:00Z", nil) // could be any http method that has a body ie: GET, PUT, DELETE
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
	req := httptest.NewRequest("GET", "localhost:8080?task-type=fee-campaign&from=2018-05-01T00:00:00Z", nil)
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

	req.From = time.Now()
	err = req.validate()
	assert.NotNil(t, err) // to value required if for value is not provided

	req.To = time.Now()
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
		httpmock.NewStringResponder(http.StatusOK, "app ok"))
	httpmock.RegisterResponder("GET", "http://null",
		httpmock.NewErrorResponder(errors.New("connection refused")))

	fn := func(args ...interface{}) (interface{}, error) {
		w := httptest.NewRecorder()
		req := args[0].(*http.Request)
		hm.handleStatus(w, req)

		if w.Code != http.StatusOK {
			return nil, errors.New(w.Body.String())
		}
		return w.Body.String(), nil
	}
	cases := trial.Cases{
		"successful call": {
			Input:    httptest.NewRequest("GET", "http://path/status?app=valid", nil),
			Expected: "app ok",
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
