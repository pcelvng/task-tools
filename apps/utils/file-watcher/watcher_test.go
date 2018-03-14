package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// disable app logging
	log.SetOutput(ioutil.Discard)

	os.Exit(m.Run())
}

func TestRun(t *testing.T) {
	os.Args = append(os.Args, "-config=test.toml")
	createTomlFile("test.toml")
	now := time.Now()
	path := now.Format("2006/01/02/15/")
	createTestWatchFile(path, "test.file.txt")

	var err error
	go func() {
		err = run()
	}()
	time.Sleep(time.Second)
	sigChan <- syscall.SIGQUIT
	if err != nil {
		t.Fail()
		t.Errorf("error should not have been returned %v", err)
	}
	defer os.Remove("test.toml")
	defer os.Remove("out.tsks.json") // default tasks out file
	defer os.Remove("test.file.txt")
	defer os.Remove(path + "test.file.txt")
	defer os.Remove("files") // default topic for the files bus
	defer os.RemoveAll(path[:4])
}

func createTestWatchFile(path, filename string) {
	err := os.MkdirAll(path, 0766)
	if err != nil {
		panic("dir could not be created " + err.Error())
	}
	err = ioutil.WriteFile(path+filename,
		[]byte(`this is a testing file`), 0666)
	if err != nil {
		panic("test toml file could not be created " + err.Error())
	}
}

func createTomlFile(filename string) {
	err := ioutil.WriteFile(filename,
		[]byte(`
bus = "file" # "" (stdout), "stdout" (default), "file", "nsq"
[[rule]]
path_template = "{YYYY}/{MM}/{DD}/{HH}"`), 0666)
	if err != nil {
		panic("test toml file could not be created" + err.Error())
	}
}

func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
