package file

import (
	"io"

	"github.com/pcelvng/task-tools/file/stat"
)

type Scanner struct {
	reader Reader
	err    error
	line   []byte
}

func NewScanner(r Reader) *Scanner {
	return &Scanner{
		reader: r,
	}
}

func (s *Scanner) Scan() bool {
	s.line, s.err = s.reader.ReadLine()
	if s.err != nil {
		if s.err == io.EOF {
			return len(s.line) > 0
		} else {
			return false
		}
	}
	if len(s.line) == 0 && s.err != io.EOF {
		return s.Scan()
	}
	return true
}

func (s *Scanner) Bytes() []byte {
	return s.line
}

func (s *Scanner) Text() string {
	return string(s.line)
}

func (s *Scanner) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

func (s *Scanner) Stats() stat.Stats {
	return s.reader.Stats()
}
