package nop

import (
	"io"
	"testing"
)

func TestReader(t *testing.T) {
	r, err := NewReader("nop://readline_eof")
	if err != nil {
		t.Error(err)
	}

	for ln, err := r.ReadLine(); err != io.EOF; ln, err = r.ReadLine() {
		t.Log(r.sts.LineCnt, string(ln), err)
		if r.sts.LineCnt > 10 {
			break
		}
	}
}

func TestListFiles(t *testing.T) {
	pth := "nop://fake/path.txt"
	allSts, err := ListFiles(pth)
	// just one sts returned with pth repeated back
	if len(allSts) == 1 {
		if allSts[0].Path != pth {
			t.Errorf("expected '%s' but got '%s'\n", pth, allSts[0].Path)
		}
	} else {
		t.Errorf("expected 1 sts but got '%v'\n", len(allSts))
	}

	// err should be nil
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	}

	// test err != nil
	pth = "nop://err/"
	allSts, err = ListFiles(pth)
	if len(allSts) != 0 {
		t.Errorf("expected 0 sts but got '%v'\n", len(allSts))
	}

	if err == nil {
		t.Error("expected err but got nil")
	}
}
