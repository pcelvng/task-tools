package file

import (
	"errors"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"

	"github.com/pcelvng/task-tools/file/stat"
)

var wd string

func TestMain(m *testing.M) {
	log.SetFlags(log.Lshortfile)

	// setup local files test
	wd, _ = os.Getwd()

	// create local test directories and files
	pths := []string{
		"test/file-1.txt",
		"test/file2.txt",
		"test/file3.gz",
		"test/f1/file4.gz",
		"test/f3/file5.txt",
		"test/f5/file-6.txt",
		"test/other/name/z1/file1.txt",
	}
	os.MkdirAll("./test/f1", 0750)
	os.MkdirAll("./test/f3", 0750)
	os.MkdirAll("./test/f5", 0750)
	os.MkdirAll("./test/other/name/z1", 0750)
	os.MkdirAll("./test/other/name/z2", 0750)
	for _, pth := range pths {
		if err := createFile("./"+pth, nil); err != nil {
			log.Fatal(err)
		}
	}

	code := m.Run()

	// cleanup
	os.RemoveAll("./test/")
	os.Exit(code)
}

func TestGlob_Local(t *testing.T) {
	fn := func(input string) ([]string, error) {
		sts, err := Glob(input, nil)
		files := make([]string, len(sts))
		for i := 0; i < len(sts); i++ {
			files[i] = strings.Replace(sts[i].Path, wd, ".", -1)
		}
		return files, err
	}
	cases := trial.Cases[string, []string]{
		"folder_file_pattern": { // this is the bug where the last folder name is dropped
			Input:    "./test/other/*/z1/f*.txt",
			Expected: []string{"./test/other/name/z1/file1.txt"},
		},
		"star.txt": {
			Input:    "./test/*.txt",
			Expected: []string{"./test/file-1.txt", "./test/file2.txt"},
		},
		"file?.txt": {
			Input:    "./test/file?.txt",
			Expected: []string{"./test/file2.txt"},
		},
		"file?.star": {
			Input:    "./test/file?.*",
			Expected: []string{"./test/file2.txt", "./test/file3.gz"},
		},
		"folders": {
			Input:    "./test/*/*",
			Expected: []string{"./test/f1/file4.gz", "./test/f3/file5.txt", "./test/f5/file-6.txt"},
		},
		"range": {
			Input:    "test/f[1-3]/*",
			Expected: []string{"./test/f1/file4.gz", "./test/f3/file5.txt"},
		},
		"folder/star.txt": {
			Input:    "test/*/*.txt",
			Expected: []string{"./test/f3/file5.txt", "./test/f5/file-6.txt"},
		},
		"file": {
			Input:    "test/file2.txt",
			Expected: []string{"./test/file2.txt"},
		},
		"nop/file": {
			Input:    "nop://file.txt", //NOTE nop is hard-coded to return file.txt
			Expected: []string{"nop://file.txt"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestIterStruct(t *testing.T) {
	fn := func(path string) (stat.Stats, error) {
		it := NewIterator(path, nil)
		for range it.Lines() {
		}
		return it.Stats(), it.Error()
	}
	cases := trial.Cases[string, stat.Stats]{
		"full file": {
			Input: "../internal/test/nop.sql",
			Expected: stat.Stats{
				LineCnt: 1,
				ByteCnt: 25,
				Size:    25,
			},
		},
		"init_err": {
			Input:       "nop://init_err",
			ExpectedErr: errors.New("init_err"),
		},
		"read_err": {
			Input:       "nop://readline_err",
			ExpectedErr: errors.New("readline_err"),
		},
		"close_err": {
			Input:       "nop://close_err",
			ExpectedErr: errors.New("close_err"),
		},
	}

	trial.New(fn, cases).
		Timeout(time.Second).
		Comparer(trial.EqualOpt(trial.IgnoreFields("Checksum", "Created", "Path"))).
		SubTest(t)
}

func createFile(pth string, opt *Options) error {
	w, err := NewWriter(pth, opt)
	if err != nil {
		return err
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	return nil
}
