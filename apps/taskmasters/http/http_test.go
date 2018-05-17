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

	"github.com/pcelvng/task/bus"
	"github.com/stretchr/testify/assert"
)

type mockResponseWriter struct{}

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
	opts.handleRequest(w, req)

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

	if opts.Bus != "-" {
		opts.producer, _ = bus.NewProducer(opts.Options)
	}

	opts.handleRequest(w, req)

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

	opts.producer, _ = bus.NewProducer(opts.Options)

	opts.handleRequest(w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `{"type":"batcher"`)
}

func TestValidationRequestError(t *testing.T) {
	req := httptest.NewRequest("GET", "localhost:8080?task-type=fee-campaign", nil) // could be any http method that has a body ie: GET, PUT, DELETE
	w := httptest.NewRecorder()
	opts := newOptions()

	opts.handleRequest(w, req)
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

	opts.handleRequest(w, req)

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
	opts.Options.NopMock = "send_err"
	opts.producer, _ = bus.NewProducer(opts.Options)

	opts.handleRequest(w, req)

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
