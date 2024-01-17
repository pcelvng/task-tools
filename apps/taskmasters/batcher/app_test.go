package main

import (
	"errors"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task/bus/nop"
)

func TestTaskMaster_generate(t *testing.T) {
	type testOpts struct {
		producerMock string
		info         string
		topic        string
	}
	fn := func(tOpts testOpts) ([]string, error) {
		p, _ := nop.NewProducer(tOpts.producerMock)
		tm := &taskMaster{
			producer: p,
			stats:    stats{Requests: make(map[string]int)},
		}
		err := tm.generate(tOpts.info)
		return p.Messages[tOpts.topic], err
	}

	trial.New(fn, trial.Cases[testOpts, []string]{
		"batch 2 days with meta": {
			Input: testOpts{
				info:  "?task-type=test&meta=job:job_name&from=2021-04-01T00&for=48h&daily#?date={yyyy}-{mm}-{dd}",
				topic: "test",
			},
			Expected: []string{`"meta":"batcher=true&job=job_name"`, `"meta":"batcher=true&job=job_name"`},
		},
		"batch 2 days task": {
			Input: testOpts{
				info:  "?task-type=test&from=2021-04-01T00&for=48h&daily#?date={yyyy}-{mm}-{dd}",
				topic: "test",
			},
			Expected: []string{`{"type":"test","info":"?date=2021-04-01"`, `{"type":"test","info":"?date=2021-04-02"`},
		},
		"batch 2 hours task": {
			Input: testOpts{
				info:  "?task-type=test&from=2018-05-10T00&for=2h#?date={yyyy}-{mm}-{dd}T{hh}",
				topic: "test",
			},
			Expected: []string{`{"type":"test","info":"?date=2018-05-10T00"`, `{"type":"test","info":"?date=2018-05-10T01"`, `{"type":"test","info":"?date=2018-05-10T02"`},
		},
		"missing type": {
			Input:       testOpts{info: "?from=2018-05-10T00&for=2h#?date={yyyy}-{mm}-{dd}T{hh}"},
			ExpectedErr: errors.New("type is required"),
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
			Expected: []string{`{"type":"test","info":"?date=2018-05-10T00"`, `{"type":"test","info":"?date=2018-05-10T01"`, `{"type":"test","info":"?date=2018-05-10T02"`},
		},
		"file batch": {
			Input: testOpts{
				info:  "test/data.json?task-type=test#?president={meta:name}&start={meta:start}&end={meta:end}",
				topic: "test",
			},
			Expected: []string{
				`?president=george washington&start=1789&end=1797`,
				`?president=john adams&start=1797&end=1801`,
				`?president=thomas jefferson&start=1801&end=1809`,
				`?president=james madison&start=1809&end=1817`,
			},
		},
		"producer failed": {
			Input: testOpts{
				info:         "?task-type=test&from=2018-05-10T00&for=2h#?date={yyyy}-{mm}-{dd}T{hh}",
				topic:        "test",
				producerMock: "send_err",
			},
			ShouldErr: true,
		},
	}).EqualFn(trial.Contains).SubTest(t)
}
