package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jbsmith7741/trial"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task/bus"
)

func TestMain(m *testing.M) {
	fOpt = file.NewOptions()
	os.Exit(m.Run())
}

func TestOptions_Validate(t *testing.T) {
	fn := func(args ...interface{}) (interface{}, error) {
		opt := args[0].(infoOptions)
		return nil, (&opt).validate()
	}
	cases := trial.Cases{
		"valid options": {
			Input: infoOptions{DestTemplate: "nop://", SrcPath: "nop://"},
		},
		"missing destination": {
			Input:     infoOptions{},
			ShouldErr: true,
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestWorker_DoTask_Err(t *testing.T) {
	// setup
	nopProducer, _ := bus.NewProducer(bus.NewOptions("nop"))
	cnclCtx, cncl := context.WithCancel(context.Background())
	cncl()

	pths := []string{
		"./test/test.json",
	}

	createdDates := []time.Time{
		time.Date(2016, 01, 01, 00, 00, 00, 00, time.UTC),
		time.Date(2017, 01, 01, 00, 00, 00, 00, time.UTC),
		time.Date(2018, 01, 01, 00, 00, 00, 00, time.UTC),
	}
	// line sets
	lineSets := [][]string{
		{
			`{"f1":"v1","f2":"v1","f3":"v1"}`,
			`{"f1":"v2","f2":"v1","f3":"v2"}`,
		},
	}

	// scenario 1 file
	createFile(lineSets[0], pths[0], createdDates[0])

	// case1: single file with duplicates
	type scenario struct {
		appOpt         *options
		producer       bus.Producer
		ctx            context.Context
		info           string
		expectedResult task.Result
		expectedMsg    string
	}
	scenarios := []scenario{
		// scenario :  bad info (no dest-template)
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           `?fields=f1`,
			expectedResult: task.ErrResult,
			expectedMsg:    `dest-template required`,
		},

		// scenario :  bad info (bad sep fields)
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           `?fields=f1&dest-template=./test/test.json`,
			expectedResult: task.ErrResult,
			expectedMsg:    `src-path required`,
		},

		// scenario :  file does not exist
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "./test/doesnotexist.json?fields=0&dest-template=./test/test.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `no such file or directory`,
		},

		// scenario :  cancelled by context
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            cnclCtx, // already cancelled
			info:           "nop://test/test.json?fields=f1&dest-template=nop://test/output.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `task interrupted`,
		},

		// scenario: err closing writer
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "./test/test.json?fields=f1&dest-template=nop://close_err/test.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `close_err`,
		},

		// scenario: err writer init
		{
			appOpt:         &options{},
			producer:       nopProducer,
			ctx:            context.Background(),
			info:           "./test/test.json?fields=f1&dest-template=nop://init_err/test.json",
			expectedResult: task.ErrResult,
			expectedMsg:    `init_err`,
		},
	}

	for sNum, s := range scenarios {
		appOpt := s.appOpt
		producer = s.producer
		wkr := appOpt.newWorker(s.info)
		gotRslt, gotMsg := wkr.DoTask(s.ctx)

		// check result
		if gotRslt != s.expectedResult {
			t.Errorf("scenario %v expected result '%v' but got '%v'", sNum+1, s.expectedResult, gotRslt)
		}

		// check msg
		if !strings.Contains(gotMsg, s.expectedMsg) {
			t.Errorf("scenario %v expected msg '%v' but got '%v'", sNum+1, s.expectedMsg, gotMsg)
		}
	}

	// cleanup
	os.RemoveAll("./test/")
}

// createFile creates file with lines at pth with created time as
// the created date.
func createFile(lines []string, pth string, created time.Time) {
	w, _ := file.NewWriter(pth, nil)

	for _, ln := range lines {
		w.WriteLine([]byte(ln))
	}
	w.Close()
	pth = w.Stats().Path // full path

	// set created date
	os.Chtimes(pth, created, created)
}
