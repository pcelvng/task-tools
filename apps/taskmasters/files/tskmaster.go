package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task/bus"
)

var (
	dumpPrefix = "tsk-files_"
)

func newTskMaster(appOpt *options) (*tskMaster, error) {
	// files bus
	b, err := bus.NewBus(appOpt.Options)
	if err != nil {
		return nil, err
	}

	// read-in locally dumped file objects
	// in case app was shut down.

	return &tskMaster{
		b: b,
	}, nil
}

// tskMaster is the main application runtime
// object that will watch for files
// and apply the config rules.
type tskMaster struct {
	b        *bus.Bus // files bus
	doneCncl context.CancelFunc
}

// DoWatch will accept a context for knowing if/when
// it should perform a shutdown. A context is returned
// to allow the caller to know when shutdown is complete.
func (tm *tskMaster) DoWatch(ctx context.Context) context.Context {

	// start doing
	go tm.doWatch(ctx)

	// ctx to indicate shutdown is complete
	doneCtx, doneCncl := context.WithCancel(context.Background())
	tm.doneCncl = doneCncl
	return doneCtx
}

func (tm *tskMaster) doWatch(ctx context.Context) {

}

// dumpFiles will write all in-progress to a tmp file.
// For simplicity dumpFiles just writes the json file objects
// to the default os.TmpDir with  prefix.
func dumpFiles(tmpDir string) {

}

// readinFiles will access the tmp dir and read in
// tmp files. Default is os.TmpDir.
func readinFiles(tmpDir string) ([]stat.Stats, error) {
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}

	// find matching tmp files
	glbPth := path.Join(tmpDir, dumpPrefix)
	glbPth = fmt.Sprintf("%v*", glbPth)
	pths, err := filepath.Glob(glbPth)
	if err != nil {
		return nil, err
	}

	// read in records
	allSts := make([]stat.Stats, 0)
	for _, pth := range pths {
		// reader
		r, err := file.NewReader(pth, nil)
		if err != nil {
			return nil, err
		}

		// read in file
		for {
			ln, err := r.ReadLine()
			if len(ln) > 0 {
				sts := stat.NewFromBytes(ln)
				if sts.Path != "" {
					allSts = append(allSts, sts)
				}
			}

			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
		}
	}

	// rm existing files
	for _, pth := range pths {
		os.Remove(pth)
	}

	return allSts, nil
}
