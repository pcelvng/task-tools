package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pcelvng/task-tools/file/stat"
)

func createFile(pth string) {
	w, _ := NewWriter(pth, nil)
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
}

func TestListFiles(t *testing.T) {
	// setup - create objects
	pths := []string{
		"./test/f1.txt",
		"./test/f2.txt",
		"./test/dir/f3.txt",
	}

	for _, pth := range pths {
		createFile(pth)
	}

	// test returns all files and directories in the directory (non-recursive)
	dirPth := "./test/"
	allSts, err := ListFiles(dirPth)
	if err != nil {
		t.Error(err)
	}

	if len(allSts) != 3 {
		t.Fatalf("expected 3 items (1 directory + 2 files) but got %v instead\n", len(allSts))
	}
	m := make(map[string]stat.Stats)
	for _, f := range allSts {
		_, p := filepath.Split(f.Path)
		m[p] = f
		if !f.IsDir {
			if f.Created == "" {
				t.Errorf("%s should have created date", f.Path)
			}
			if f.Size == 0 {
				t.Errorf("%s should have size", f.Path)
			}
			if f.Checksum == "" {
				t.Errorf("%s should have checksum", f.Path)
			}

		}
	}
	f, ok := m["dir"]
	if !ok {
		t.Errorf("expected dir")
	}
	if !f.IsDir {
		t.Errorf("should be dir")
	}

	// cleanup
	for _, pth := range pths {
		os.Remove(pth)
	}
	os.Remove("./test/dir/")
	os.Remove("./test")
}
