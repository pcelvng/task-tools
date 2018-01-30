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
