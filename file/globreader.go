package file

import (
	"fmt"
	"io"
	"sync"

	"github.com/pcelvng/task-tools/file/stat"
)

func NewGlobReader(path string, opts *Options) (_ Reader, err error) {
	r := &GlobReader{
		path: path,
		sts: stat.Stats{
			Path: path,
		},
	}
	if opts != nil {
		r.opts = *opts
	}
	if r.files, err = Glob(path, opts); err != nil {
		return nil, err
	}
	if err := r.nextFile(); err != nil {
		return nil, fmt.Errorf("no files found for %s", path)
	}
	r.sts.Files = int64(len(r.files))
	return r, nil
}

type GlobReader struct {
	mu sync.RWMutex

	path string
	opts Options
	sts  stat.Stats

	files     []stat.Stats
	fileIndex int
	reader    Reader
}

func (g *GlobReader) nextFile() (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.files) <= g.fileIndex {
		g.reader = nil
		return io.EOF
	}
	if g.reader != nil {
		sts := g.reader.Stats()
		g.sts.ByteCnt += sts.ByteCnt
		g.sts.LineCnt += sts.LineCnt
		g.sts.Size += sts.Size
		g.reader.Close()
	}
	g.reader, err = NewReader(g.files[g.fileIndex].Path, &g.opts)
	g.fileIndex++

	return err
}

func (g *GlobReader) Read(p []byte) (n int, err error) {
	if g.reader == nil {
		return 0, io.EOF
	}

	g.mu.RLock()
	n, err = g.reader.Read(p)
	g.mu.RUnlock()

	if err == io.EOF {
		err = g.nextFile()
	}
	return n, err
}

func (g *GlobReader) Close() error {
	if g.reader != nil {
		return g.reader.Close()
	}
	return nil
}

func (g *GlobReader) ReadLine() (b []byte, err error) {
	if g.reader == nil {
		return b, io.EOF
	}

	g.mu.RLock()
	b, err = g.reader.ReadLine()
	g.mu.RUnlock()

	if err == io.EOF {
		err = g.nextFile()
	}
	return b, err
}

func (g *GlobReader) Stats() stat.Stats {
	sts := g.sts
	if g.reader != nil {
		s := g.reader.Stats()
		sts.ByteCnt += s.ByteCnt
		sts.LineCnt += s.LineCnt
		sts.Size += s.Size
	}
	return sts
}
