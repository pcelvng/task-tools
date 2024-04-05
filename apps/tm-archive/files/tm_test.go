package main

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task/bus/io"
	"github.com/pcelvng/task/bus/nop"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
)

func TestOptions_Validate(t *testing.T) {
	fn := func(opt options) (interface{}, error) {
		return nil, opt.Validate()
	}
	trial.New(fn, trial.Cases[options, any]{
		"one rule required": {
			Input:       options{},
			ExpectedErr: errors.New("no rules provided"),
		},
		"rules require task type": {
			Input: options{
				Rules: []*Rule{
					{SrcPattern: "./test/*.txt"},
				},
			},
			ExpectedErr: errors.New("task type required for all rules"),
		},
		"rules require src pattern": {
			Input: options{
				Rules: []*Rule{
					{TaskType: "test-type"},
				},
			},
			ExpectedErr: errors.New("src_pattern required for all rules"),
		},
		/*"cron check": {
			Input: options{
				Rules: []*Rule{
					{TaskType: "test-type", SrcPattern: "./test/pattern.txt", CronCheck: "invalid"},
				},
			},
			ExpectedErr: errors.New("invalid cron"),
		},*/
	}).Test(t)
}

func TestTskMaster(t *testing.T) {
	os.Mkdir("./test", 0766)

	// setup
	// write file stats to a file
	pth := "./test/files.json"
	stats := []*stat.Stats{
		{Path: "s3://test/file1.txt", Checksum: "checksum", Created: "created", LineCnt: 9, ByteCnt: 20, Size: 20},
		{Path: "/test/file1.txt", Checksum: "checksum", Created: "created", LineCnt: 10, ByteCnt: 20, Size: 20},
		{Path: "/test/file2.txt", Checksum: "checksum", Created: "created", LineCnt: 11, ByteCnt: 20, Size: 20},
		{Path: "/test/file3.txt", Checksum: "checksum", Created: "created", LineCnt: 12, ByteCnt: 20, Size: 20},
		{Path: "/test/file4.gz", Checksum: "checksum", Created: "created", LineCnt: 13, ByteCnt: 20, Size: 20},
	}
	writeStats(pth, stats)

	// test: typical lifecycle

	opt := &options{
		Rules: []*Rule{
			{TaskType: "test-type-s3", SrcPattern: "s3://test/file*.txt", Topic: "./test/out.tsks.json"},                         // send immediately
			{TaskType: "test-type", SrcPattern: "/test/file*.gz", Topic: "./test/out.tsks.json"},                                 // send immediately
			{TaskType: "test-type-count", SrcPattern: "/test/file*.txt", CountCheck: 3, Topic: "./test/out.tsks.json"},           // send when count is reached
			{TaskType: "test-type-cron", SrcPattern: "/test/file3.txt", CronCheck: "* * * * * *", Topic: "./test/out.tsks.json"}, // send when count is reached
		},
	}
	tm := opt.new(nil).(*tskMaster)
	tm.producer = io.NewProducer()

	outTopic := "./test/out.tsks.json"
	var err error
	tm.consumer, err = io.NewConsumer(pth)
	if err != nil {
		t.Fatal(err)
	}
	doneCtx := tm.DoFileWatch(context.Background())

	<-doneCtx.Done() // wait until done processing
	r, _ := file.NewReader(outTopic, nil)

	tsks := make(map[string]*task.Task)
	for {
		ln, err := r.ReadLine()
		if len(ln) > 0 {
			tsk, _ := task.NewFromBytes(ln)
			tsks[tsk.Type] = tsk
		}
		if err != nil {
			break
		}
	}

	// should be 5 tasks created
	if r.Stats().LineCnt != 5 {
		t.Errorf("expected %v lines got %v", 5, r.Stats().LineCnt)
	}
	r.Close()

	// test-type-s3
	if tsks["test-type-s3"] == nil {
		t.Error("expected non-nil value")
	} else {
		expected := "s3://test/file1.txt?" // starts with
		info := tsks["test-type-s3"].Info
		if !strings.HasPrefix(info, expected) {
			t.Errorf("expected prefix '%v' got '%v'", expected, info)
		}
	}

	// test-type
	if tsks["test-type"] == nil {
		t.Error("expected non-nil value")
	} else {
		expected := "/test/file4.gz?" // starts with
		info := tsks["test-type"].Info
		if !strings.HasPrefix(info, expected) {
			t.Errorf("expected prefix '%v' got '%v'", expected, info)
		}
	}

	// test-type-count
	if tsks["test-type-count"] == nil {
		t.Error("expected non-nil value")
	} else {
		expected := "/test/"
		info := tsks["test-type-count"].Info
		if info != expected {
			t.Errorf("expected '%v' got '%v'", expected, info)
		}
	}

	// test-type-count
	if tsks["test-type-cron"] == nil {
		t.Error("expected non-nil value")
	} else {
		expected := "/test/"
		info := tsks["test-type-cron"].Info
		if info != expected {
			t.Errorf("expected '%v' got '%v'", expected, info)
		}
	}

	// cleanup
	os.Remove(pth)
	os.Remove(outTopic)
	os.Remove("./test")
}

func TestTskMaster_ReadFileStatsErr(_ *testing.T) {
	appOpt := &options{
		Rules: []*Rule{
			{TaskType: "test-type", SrcPattern: "/test/file.txt"},
		},
	}
	tm := appOpt.new(nil).(*tskMaster)
	tm.consumer, _ = nop.NewConsumer("msg_err")

	tm.l = log.New(ioutil.Discard, "", log.LstdFlags)
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*1)
	tm.readFileStats(ctx)
}

func TestTskMaster_SendTskErr(t *testing.T) {
	appOpt := &options{
		Rules: []*Rule{
			{TaskType: "test-type", SrcPattern: "/test/file.txt"},
		},
	}
	tm := appOpt.new(nil).(*tskMaster)
	tm.producer, _ = nop.NewProducer("send_err")

	bfr, _ := buf.NewBuffer(nil)
	tm.l = log.New(bfr, "", 0)
	tsk := task.New("test-type", "test-info")
	tm.sendTsk(tsk, appOpt.Rules[0]) //

	bfr.Close()
	b := make([]byte, 40)
	bfr.Read(b)

	expected := `send on topic 'test-type' msg 'send_err'`
	got := string(b)
	if expected != got {
		t.Errorf("expected %v got %v", expected, got)
	}
}

func TestTskMaster_ClearFiles(t *testing.T) {
	// setup
	// write file stats to a file
	stats := []*stat.Stats{
		{Path: "s3://test/file1.txt", Checksum: "checksum", Created: "created", LineCnt: 9, ByteCnt: 20, Size: 20},
		{Path: "/test/file1.txt", Checksum: "checksum", Created: "created", LineCnt: 10, ByteCnt: 20, Size: 20},
		{Path: "/test/file2.txt", Checksum: "checksum", Created: "created", LineCnt: 11, ByteCnt: 20, Size: 20},
		{Path: "/test/file3.txt", Checksum: "checksum", Created: "created", LineCnt: 12, ByteCnt: 20, Size: 20},
		{Path: "/test/file4.gz", Checksum: "checksum", Created: "created", LineCnt: 13, ByteCnt: 20, Size: 20},
	}

	// test: typical lifecycle
	appOpt := &options{
		Rules: []*Rule{
			{TaskType: "test-type-s3", SrcPattern: "s3://test/file*.txt"},                         // send immediately
			{TaskType: "test-type", SrcPattern: "/test/file*.gz"},                                 // send immediately
			{TaskType: "test-type-count", SrcPattern: "/test/file*.txt", CountCheck: 3},           // send when count is reached
			{TaskType: "test-type-cron", SrcPattern: "/test/file3.txt", CronCheck: "* * * * * *"}, // send when count is reached
		},
	}
	tm := appOpt.new(nil).(*tskMaster)
	tm.producer = io.NewNullProducer()
	for _, sts := range stats {
		tm.matchAll(sts)
	}

	tm.clearFiles()
	tm.waitClearFiles()

	if !tm.isFilesEmpty() {
		t.Error("files is not empty")
	}
}

func writeStats(pth string, stats []*stat.Stats) {
	w, _ := file.NewWriter(pth, nil)

	for _, sts := range stats {
		w.WriteLine(sts.JSONBytes())
	}
	w.Close()
}
