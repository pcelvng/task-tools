package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
)

func TestNewTskMaster(t *testing.T) {
	// test with default options
	appOpt := newOptions()
	appOpt.Rules = []*Rule{
		{TaskType: "test-type", SrcPattern: "./test/*.txt"},
	}
	tm, err := newTskMaster(appOpt)
	if err != nil {
		t.Errorf("expected nil got '%v'", err.Error())
	}

	if tm == nil {
		t.Fatal("should not be nil")
	}

	// has producer
	if tm.producer == nil {
		t.Error("should not be nil")
	}

	// has consumer
	if tm.consumer == nil {
		t.Error("should not be nil")
	}

	// check doneCtx
	if tm.doneCtx.Err() != nil {
		t.Errorf("expected nil got '%v'", err.Error())
	}

	// check doneCncl plumbing
	tm.doneCncl()
	if tm.doneCtx.Err() == nil {
		t.Error("should not be nil")
	}

	// check initialized
	if tm.files == nil {
		t.Error("should not be nil")
	}

	// check appOpt
	if tm.appOpt == nil {
		t.Error("should not be nil")
	}

	// check msgCh
	if tm.msgCh == nil {
		t.Error("should not be nil")
	}

	// check rules
	if tm.rules == nil {
		t.Error("should not be nil")
	}

	// check logger
	if tm.l == nil {
		t.Error("should not be nil")
	}
}

func TestNewTskMaster_Errs(t *testing.T) {
	// test: at least one rule required
	appOpt := newOptions()
	tm, err := newTskMaster(appOpt)
	expected := "no rules provided"
	if err == nil {
		t.Error("should not be nil")
	} else if err.Error() != expected {
		t.Errorf("expected '%v' got '%v'\n", expected, err.Error())
	}

	if tm != nil {
		t.Errorf("expected nil got '%v'", err.Error())
	}

	// test: rules require task type
	appOpt.Rules = []*Rule{
		{SrcPattern: "./test/*.txt"},
	}
	_, err = newTskMaster(appOpt)
	expected = "task type required for all rules"
	if err == nil {
		t.Error("should not be nil")
	} else if err.Error() != expected {
		t.Errorf("expected '%v' got '%v'\n", expected, err.Error())
	}

	// test: rules require src pattern
	appOpt.Rules = []*Rule{
		{TaskType: "test-type"},
	}
	_, err = newTskMaster(appOpt)
	expected = "src_pattern required for all rules"
	if err == nil {
		t.Error("should not be nil")
	} else if err.Error() != expected {
		t.Errorf("expected '%v' got '%v'\n", expected, err.Error())
	}

	// test: producer err
	appOpt.Rules = []*Rule{
		{TaskType: "test-type", SrcPattern: "./test/pattern.txt"},
	}
	appOpt.Options.OutBus = "invalid-out-bus"
	_, err = newTskMaster(appOpt)
	expected = `new producer:` // contains
	if err == nil {
		t.Error("should not be nil")
	} else if !strings.Contains(err.Error(), expected) {
		t.Errorf(`expected '%v' got '%v'`, expected, err.Error())
	}

	// test: consumer err
	appOpt.Rules = []*Rule{
		{TaskType: "test-type", SrcPattern: "./test/pattern.txt"},
	}
	appOpt.Options.InBus = "invalid-in-bus"
	appOpt.Options.OutBus = ""
	_, err = newTskMaster(appOpt)
	expected = `new consumer:` // contains
	if err == nil {
		t.Error("should not be nil")
	} else if !strings.Contains(err.Error(), expected) {
		t.Errorf(`expected '%v' got '%v'`, expected, err.Error())
	}

	// test: cron err
	appOpt.Rules = []*Rule{
		{TaskType: "test-type", SrcPattern: "./test/pattern.txt", CronCheck: "invalid"},
	}
	appOpt.Options.InBus = ""
	appOpt.Options.OutBus = ""
	_, err = newTskMaster(appOpt)
	expected = `invalid cron:` // contains
	if err == nil {
		t.Error("should not be nil")
	} else if !strings.Contains(err.Error(), expected) {
		t.Errorf(`expected '%v' got '%v'`, expected, err.Error())
	}
}

func TestTskMaster(t *testing.T) {
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
	appOpt := newOptions()
	appOpt.Bus = "file"
	appOpt.InFile = pth
	appOpt.OutFile = "./test/out.tsks.json"
	appOpt.Rules = []*Rule{
		{TaskType: "test-type-s3", SrcPattern: "s3://test/file*.txt", Topic: "test-topic"},  // send immediately
		{TaskType: "test-type", SrcPattern: "/test/file*.gz"},                               // send immediately
		{TaskType: "test-type-count", SrcPattern: "/test/file*.txt", CountCheck: 3},         // send when count is reached
		{TaskType: "test-type-cron", SrcPattern: "/test/file3.txt", CronCheck: "* * * * *"}, // send when count is reached
	}
	tm, _ := newTskMaster(appOpt)
	doneCtx := tm.DoFileWatch(context.Background())

	<-doneCtx.Done() // wait until done processing
	r, _ := file.NewReader(appOpt.OutFile, nil)

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
	os.Remove(appOpt.OutFile)
	os.Remove("./test")
}

func TestTskMaster_ReadFileStatsErr(t *testing.T) {
	appOpt := newOptions()
	appOpt.Bus = "nop"
	appOpt.NopMock = "msg_err"
	appOpt.Rules = []*Rule{
		{TaskType: "test-type", SrcPattern: "/test/file.txt"},
	}
	tm, _ := newTskMaster(appOpt)
	tm.l = log.New(ioutil.Discard, "", log.LstdFlags)
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond*1)
	tm.readFileStats(ctx)
}

func TestTskMaster_SendTskErr(t *testing.T) {
	appOpt := newOptions()
	appOpt.OutBus = "nop"
	appOpt.NopMock = "send_err"
	appOpt.Rules = []*Rule{
		{TaskType: "test-type", SrcPattern: "/test/file.txt"},
	}
	tm, _ := newTskMaster(appOpt)

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
	appOpt := newOptions()
	appOpt.InBus = "stdin"
	appOpt.OutBus = "file"
	appOpt.OutFile = "/dev/null"
	appOpt.Rules = []*Rule{
		{TaskType: "test-type-s3", SrcPattern: "s3://test/file*.txt"},                       // send immediately
		{TaskType: "test-type", SrcPattern: "/test/file*.gz"},                               // send immediately
		{TaskType: "test-type-count", SrcPattern: "/test/file*.txt", CountCheck: 3},         // send when count is reached
		{TaskType: "test-type-cron", SrcPattern: "/test/file3.txt", CronCheck: "* * * * *"}, // send when count is reached
	}
	tm, _ := newTskMaster(appOpt)
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
