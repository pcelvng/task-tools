package local

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pcelvng/task-tools/file/util"
)

func TestStat(t *testing.T) {
	//setup
	fPath, f, _ := util.OpenTmp("./tmp", "")
	fmt.Fprintln(f, "Hello World")
	f.Close()
	//remove tmp files
	defer os.Remove(fPath)
	d, _ := filepath.Split(fPath)

	t.Run("directory", func(t *testing.T) {
		s, err := Stat(d)
		if err != nil {
			t.Error("directory", err)
		}
		if s.Size == 0 || s.Path == "" || s.Created == "" {
			t.Error("directory stats: not set", s.JSONString())
		}
		if !s.IsDir {
			t.Error("dir: incorrect file type")
		}
	})

	t.Run("file", func(t *testing.T) {
		s, err := Stat(fPath)
		if err != nil {
			t.Error("file", err)
		}
		if s.Size == 0 || s.Path == "" || s.Created == "" || s.Checksum == "" {
			t.Error("file stats: not set", s.JSONString())
		}
		if s.IsDir {
			t.Error("file: incorrect file type")
		}
	})

	t.Run("missing", func(t *testing.T) {
		_, err := Stat(d + "/missing")
		if err == nil {
			t.Error("Expected error on missing file")
		}
	})
}

func TestRmLocalPrefix(t *testing.T) {
	type scenario struct {
		pth      string
		expected string // expected pth
	}
	scenarios := []scenario{
		{pth: "./pth/to/file.txt", expected: "./pth/to/file.txt"},
		{pth: "/pth/to/file.txt", expected: "/pth/to/file.txt"},
		{pth: "file.txt", expected: "file.txt"},
		{pth: "./file.txt", expected: "./file.txt"},
		{pth: "local://pth/to/file.txt", expected: "/pth/to/file.txt"},
		{pth: "local://./pth/to/file.txt", expected: "./pth/to/file.txt"},
		{pth: "local:///pth/to/file.txt", expected: "/pth/to/file.txt"},
		{pth: "local://file.txt", expected: "/file.txt"},
	}

	for _, sc := range scenarios {
		actual := rmLocalPrefix(sc.pth)
		if actual != sc.expected {
			t.Errorf("got '%v' but expected '%v'", actual, sc.expected)
		}
	}
}
