package main

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task/bus"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// disable app logging
	log.SetOutput(ioutil.Discard)

	os.Exit(m.Run())
}

func TestProducerErr(t *testing.T) {
	o := &options{}
	o.Options = &bus.Options{}
	o.Bus = "nop"
	o.NopMock = "init_err"
	o.Rules = append(o.Rules, &Rule{})

	_, err := newWatchers(o)
	assert.NotNil(t, err)
}

func TestNewWatchers(t *testing.T) {
	o := &options{}
	o.Options = &bus.Options{}
	o.Bus = "nop"
	o.NopMock = "msg_msg_done"
	o.Rules = append(o.Rules, &Rule{}, &Rule{})

	ws, _ := newWatchers(o)
	assert.Equal(t, 2, len(ws))
}

func TestErrOnStop(t *testing.T) {
	o := &options{}
	o.Options = &bus.Options{}
	o.Bus = "nop"
	o.NopMock = "stop_err"
	o.Rules = append(o.Rules, &Rule{}, &Rule{})

	ws1, _ := newWatchers(o)
	err := closeWatchers(ws1)
	assert.NotNil(t, err)

	o.NopMock = "msg_msg_done"
	ws2, _ := newWatchers(o)
	err = closeWatchers(ws2)
	assert.Nil(t, err)
}

func TestBadFrequency(t *testing.T) {
	o := &options{}
	o.Options = &bus.Options{}
	o.Bus = "nop"
	o.NopMock = "msg_msg_done"
	o.Rules = append(o.Rules, &Rule{}, &Rule{})

	ws1, _ := newWatchers(o)

	ws1[0].frequency = "bad"
	err := ws1[0].runWatch()
	assert.NotNil(t, err)
}

func TestRunWatch(t *testing.T) {
	var err error
	o := &options{}
	o.Options = &bus.Options{}
	o.Bus = "nop"
	o.NopMock = "msg_msg_done"
	o.Rules = append(o.Rules, &Rule{}, &Rule{})

	ws1, _ := newWatchers(o)
	go func() {
		err = ws1[0].runWatch()
	}()

	time.Sleep(time.Second)
	ws1[0].close()

	assert.Nil(t, err)
}

func TestCurrentFilesErr(t *testing.T) {
	o := &options{}
	o.Options = &bus.Options{}
	o.Bus = "nop"
	o.NopMock = "msg_msg_done"
	o.Rules = append(o.Rules, &Rule{}, &Rule{})

	ws1, _ := newWatchers(o)
	err := ws1[0].currentFiles("nop://err")
	assert.NotNil(t, err)
}

func TestCompareFileList(t *testing.T) {
	curCache := make(fileList)
	curFiles := make(fileList)

	curFiles["new_file_one"] = &stat.Stats{
		Size:    123456,
		Created: "2018-04-12T14:03:53Z",
	}

	curFiles["new_file_two"] = &stat.Stats{
		Size:    321654,
		Created: "2018-04-12T14:03:53Z",
	}

	curFiles["new_file_three"] = &stat.Stats{}

	newFiles := compareFileList(curCache, curFiles)

	// reset for new  cache
	curCache = curFiles
	curFiles = make(fileList)

	assert.Equal(t, 2, len(newFiles))

	// date changed should add this as a new file
	curFiles["new_file_one"] = &stat.Stats{
		Size:    123456,
		Created: "2018-04-12T14:44:53Z",
	}

	// nothing changed, should not add this file as a new file
	curFiles["new_file_two"] = &stat.Stats{
		Size:    321654,
		Created: "2018-04-12T14:03:53Z",
	}

	// file changed with new size and created, should show as a new file
	curFiles["new_file_three"] = &stat.Stats{
		Size:    1234,
		Created: "2018-04-12T14:44:53Z",
	}

	// new file should be added in the new file list
	curFiles["new_file_four"] = &stat.Stats{
		Created: "2018-04-12T14:41:53Z",
	}

	newFiles = compareFileList(curCache, curFiles)

	assert.Equal(t, 3, len(newFiles))
	assert.Equal(t, newFiles["new_file_four"], curFiles["new_file_four"])
}
