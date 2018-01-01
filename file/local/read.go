package local

import (
	"bufio"
	"os"

	"github.com/pcelvng/task-tools/file/stat"
)

func NewReader(pth string) (*Reader, error) {
	f, err := os.Open(pth)
	if err != nil {
		return nil, err
	}

	rBuf := bufio.NewReader(f)
	return &Reader{
		f:    f,
		rBuf: rBuf,
		sts:  stat.New(),
	}, nil
}

type Reader struct {
	f    *os.File
	rBuf *bufio.Reader
	sts  stat.Stat
}

func (r *Reader) ReadLine() (ln []byte, err error) {
	ln, err = r.rBuf.ReadBytes('\n')

	if len(ln) > 0 {
		r.sts.AddLine()
		r.sts.AddBytes(int64(len(ln)))

		if ln[len(ln)-1] == '\n' {
			return ln[:len(ln)-1], err
		}
	}
	return
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.rBuf.Read(p)
	r.sts.AddBytes(int64(n))
	return
}

func (r *Reader) Stats() stat.Stat {
	return r.sts.Clone()
}

func (r *Reader) Close() error {
	r.f.Close()
	return nil
}
