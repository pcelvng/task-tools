package nop

import (
	"errors"
	"io"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
)

// MsgChan can be used to set a read message
// value. The call to Read or ReadLine will block
// until a mock message is send on MsgChan.
// This way both the value and timing of a read call
// can be controlled. This can be useful when testing.
//
// The reader only listens on MsgChan if len(MockLine)
// is 0.
//
// If EOFChan is closed then and a message is written to
// MsgChan the reader may behave as if it reached an
// end-of-file or it may return the MsgChan message.
// If it is desired to return a message with end-of-file
// behavior then make sure MockLine is set with the desired
// return value.
var MsgChan = make(chan []byte)

// EOFChan can be closed to simulate reading the end of a file.
// The MockLine value will be returned with simulated end-of-file
// behavior.
var EOFChan = make(chan interface{})

// MockLine can be used for setting the return value of
// a call to Read() or ReadLine(). The set value will
// be returned unless Reader is initialized with a
// MockReader value that indicates returning an error.
var MockLine = []byte("mock line\n")

// MockReadMode can be set in order to
// mock various return scenarios.
//
// MockReadMode can be set directly on module
// or through the NewReader initializer. The MockReadMode
// value is the string value right after 'nop://'.
//
// Example Initializer Paths:
// "nop://init_err/" - MockReadMode is set as 'init_err'
// "nop://err" - MockReadMode is set as 'err'
// "nop://read_err/other/fake/path.txt" - MockReadMode is set as 'read_err'
//
// Supported Values:
// - "init_err" - returns err on NewReader
// - "err" - every method than can, returns an error
// - "read_err" - returns err on Reader.Read() call.
// - "read_EOF" - returns io.EOF on Reader.Read() call.
// - "readline_err" - returns err on Reader.ReadLine() call.
// - "readline_EOF" - returns io.EOF on Reader.ReadLine() call.
// - "close_err" - returns non-nil error on Reader.Close() call.

/*// MockCreatedDate represents the date the mock file is created.
// The default is just the zero value of time.Time.
var MockCreatedDate = time.Time{}*/

func NewReader(pth string) (*Reader, error) {
	sts := stat.New()
	sts.SetPath(pth)
	sts.SetCreated(time.Now())

	r := &Reader{sts: sts}
	// set MockReader
	mockReadMode, _ := url.Parse(pth)
	if mockReadMode != nil {
		r.MockReadMode = mockReadMode.Host
	}

	if r.MockReadMode == "init_err" {
		return nil, errors.New(r.MockReadMode)
	}

	return r, nil
}

type Reader struct {
	sts          stat.Stats
	MockReadMode string
}

// Read will return n as len(MockLine) or length
// of MsgChan bytes.
func (r *Reader) Read(p []byte) (n int, err error) {
	switch strings.ToLower(r.MockReadMode) {
	case "read_err", "err":
		return n, errors.New(r.MockReadMode)
	case "read_eof":
		return n, io.EOF
	}

	writefn := func(msg []byte) int {
		cnt := int(math.Min(float64(len(msg)), float64(len(p))))
		p = msg[:cnt]
		return cnt
	}

	// use MsgChan if MockLine has
	// no value.
	if len(MockLine) == 0 {
		msg := <-MsgChan
		r.sts.AddBytes(int64(len(msg)))
		return writefn(msg), nil
	}

	defer r.sts.AddBytes(int64(len(MockLine)))
	select {
	case <-EOFChan: // EOF if EOFChan is closed
		return writefn(MockLine), io.EOF
	default:
		return writefn(MockLine), nil
	}
}

func (r *Reader) ReadLine() (ln []byte, err error) {
	switch strings.ToLower(r.MockReadMode) {
	case "readline_err", "err":
		return ln, errors.New(r.MockReadMode)
	case "readline_eof":
		return ln, io.EOF
	}
	defer r.sts.AddLine()

	// use MsgChan if MockLine has
	// no value.
	if len(MockLine) == 0 {
		msg := <-MsgChan
		r.sts.AddBytes(int64(len(msg)))

		return msg, nil
	}

	defer r.sts.AddBytes(int64(len(MockLine)))
	select {
	case <-EOFChan: // EOF if EOFChan is closed
		return MockLine, io.EOF
	default:
		return MockLine, nil
	}
}

func (r *Reader) Stats() stat.Stats {
	return r.sts.Clone()
}

func (r *Reader) Close() (err error) {
	r.sts.SetSize(r.sts.ByteCnt)

	if r.MockReadMode == "close_err" || r.MockReadMode == "err" {
		return errors.New(r.MockReadMode)
	}
	return nil
}

// ListFiles will return a single stat.Stats record
// whose path is the same as pth.
//
// if pth has 'err' in the first part of the path
// then no stats will be returned along with an
// instance of error.
func ListFiles(pth string) ([]stat.Stats, error) {
	sts := stat.New()

	// determine to return err
	errMode, _ := url.Parse(pth)
	if errMode != nil && errMode.Host == "err" {
		return nil, errors.New(errMode.Host)
	}
	sts.SetPath(pth)

	return []stat.Stats{sts}, nil
}
