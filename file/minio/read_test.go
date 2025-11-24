package minio

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pcelvng/task-tools/file/stat"
)

func TestListFiles(t *testing.T) {
	// setup - create objects
	pths := []string{
		fmt.Sprintf("mcs://%s/list-test/f1.txt", testBucket),
		fmt.Sprintf("mcs://%s/list-test/f2.txt", testBucket),
		fmt.Sprintf("mcs://%s/list-test/dir/f3.txt", testBucket),
	}

	for _, pth := range pths {
		createTestFile(pth)
	}

	// test returns only files
	dirPth := fmt.Sprintf("mcs://%s/list-test/", testBucket)
	allSts, err := ListFiles(dirPth, testOption)
	if err != nil {
		t.Error(err)
	}

	if len(allSts) != 3 {
		t.Fatalf("expected 3 files but got %v instead\n", len(allSts))
	}
	m := make(map[string]stat.Stats)
	for _, f := range allSts {
		m[strings.Replace(f.Path, dirPth, "", -1)] = f
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
	f, ok := m["dir/"]
	if !ok {
		s := []string{}
		for key := range m {
			s = append(s, key)
		}
		t.Errorf("missing dir/ path, found %v", s)
	}
	if !f.IsDir {
		t.Errorf("should be dir")
	}

	// test that missing trailing "/" has same results
	dirPth = fmt.Sprintf("mcs://%s/list-test", testBucket)
	allSts, err = ListFiles(dirPth, testOption)
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	if len(allSts) != 3 {
		t.Fatalf("expected 3 files but got %v instead\n", len(allSts))
	}
	m = make(map[string]stat.Stats)
	for _, f := range allSts {
		m[strings.Replace(f.Path, dirPth, "", -1)] = f
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
	f, ok = m["/dir/"]
	if !ok {
		s := []string{}
		for key := range m {
			s = append(s, key)
		}
		t.Errorf("missing /dir/ path, found %v", s)
	}
	if !f.IsDir {
		t.Errorf("should be dir")
	}

	// test bad s3 client
	_, err = ListFiles(dirPth, Option{Host: "bad/endpoint"})
	if err == nil {
		t.Error("expected err but got nil instead")
	}

	// cleanup
	for _, pth := range pths {
		rmTestFile(pth)
	}
}
