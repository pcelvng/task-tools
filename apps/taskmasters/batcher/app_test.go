package main

import (
	"errors"
	"testing"

	"github.com/jbsmith7741/trial"
	"github.com/pcelvng/task/bus/nop"
)

func TestTaskMaster_generate(t *testing.T) {
	type testOpts struct {
		producerMock string
		info         string
		topic        string
	}
	fn := func(args ...interface{}) (interface{}, error) {
		tOpts := args[0].(testOpts)
		p, _ := nop.NewProducer(tOpts.producerMock)
		tm := &taskMaster{
			producer: p,
			stats:    stats{Requests: make(map[string]int)},
		}
		err := tm.generate(tOpts.info)
		return p.Messages[tOpts.topic], err
	}

	trial.New(fn, map[string]trial.Case{
		"batch 2 days with meta": {
			Input: testOpts{
				info:  "?task-type=test&meta=job:job_name&from=2021-04-01T00&for=48h&daily#?date={yyyy}-{mm}-{dd}",
				topic: "test",
			},
			Expected: producerResponse(`"meta":"batcher=true&job=job_name"`, `"meta":"batcher=true&job=job_name"`),
		},

		"batch 2 days task": {
			Input: testOpts{
				info:  "?task-type=test&from=2021-04-01T00&for=48h&daily#?date={yyyy}-{mm}-{dd}",
				topic: "test",
			},
			Expected: producerResponse(`{"type":"test","info":"?date=2021-04-01"`, `{"type":"test","info":"?date=2021-04-02"`),
		},
		"batch 2 hours task": {
			Input: testOpts{
				info:  "?task-type=test&from=2018-05-10T00&for=2h#?date={yyyy}-{mm}-{dd}T{hh}",
				topic: "test",
			},
			Expected: producerResponse(`{"type":"test","info":"?date=2018-05-10T00"`, `{"type":"test","info":"?date=2018-05-10T01"`, `{"type":"test","info":"?date=2018-05-10T02"`),
		},
		"missing type": {
			Input:       testOpts{info: "?from=2018-05-10T00&for=2h#?date={yyyy}-{mm}-{dd}T{hh}"},
			ExpectedErr: errors.New("type is required"),
		},
		"missing from": {
			Input:       testOpts{info: "?task-type=test&for=2h#?date={yyyy}-{mm}-{dd}T{hh}"},
			ExpectedErr: errors.New("from is required"),
		},
		"for or to is required": {
			Input:       testOpts{info: "?task-type=test&from=2018-05-10T00#?date={yyyy}-{mm}-{dd}T{hh}"},
			ExpectedErr: errors.New("end date required"),
		},
		"batch 2 hours override topic": {
			Input: testOpts{
				info:  "?task-type=test&topic=topic&from=2018-05-10T00&for=2h#?date={yyyy}-{mm}-{dd}T{hh}",
				topic: "topic",
			},
			Expected: producerResponse(`{"type":"test","info":"?date=2018-05-10T00"`, `{"type":"test","info":"?date=2018-05-10T01"`, `{"type":"test","info":"?date=2018-05-10T02"`),
		},
		"producer failed": {
			Input: testOpts{
				info:         "?task-type=test&from=2018-05-10T00&for=2h#?date={yyyy}-{mm}-{dd}T{hh}",
				topic:        "test",
				producerMock: "send_err",
			},
			ShouldErr: true,
		},
	}).EqualFn(trial.Contains).Test(t)
}

func producerResponse(args ...string) []string {
	return args
}
