package nop

import (
	"errors"
	"io"
	"net/url"
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
// - "readline_err" - returns err on Reader.ReadLine() call.
// - "close_err" - returns non-nil error on Reader.Close() call.
var MockReadMode string

// MockCreatedDate represents the date the mock file is created.
// The default is just the zero value of time.Time.
var MockCreatedDate = time.Time{}

func NewReader(pth string) (*Reader, error) {
	sts := stat.New()
	sts.SetPath(pth)
	sts.SetCreated(time.Now())

	// set MockReader
	mockReadMode, _ := url.Parse(pth)
	if mockReadMode != nil {
		MockReadMode = mockReadMode.Host
	}

	if MockReadMode == "init_err" {
		return nil, errors.New(MockReadMode)
	}

	return &Reader{
		sts: sts,
	}, nil
}

type Reader struct {
	sts stat.Stat
}

// Read will return n as len(MockLine) or length
// of MsgChan bytes.
func (r *Reader) Read(p []byte) (n int, err error) {
	if MockReadMode == "read_err" || MockReadMode == "err" {
		return n, errors.New(MockReadMode)
	}

	// use MsgChan if MockLine has
	// no value.
	if len(MockLine) == 0 {
		msg := <-MsgChan
		r.sts.AddBytes(int64(len(msg)))
		return len(msg), nil
	}

	defer r.sts.AddBytes(int64(len(MockLine)))
	select {
	case <-EOFChan: // EOF if EOFChan is closed
		return len(MockLine), io.EOF
	default:
		return len(MockLine), nil
	}
}

func (r *Reader) ReadLine() (ln []byte, err error) {
	if MockReadMode == "readline_err" || MockReadMode == "err" {
		return ln, errors.New(MockReadMode)
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

func (r *Reader) Stats() stat.Stat {
	return r.sts.Clone()
}

func (r *Reader) Close() (err error) {
	r.sts.SetSize(r.sts.ByteCnt)

	if MockReadMode == "close_err" || MockReadMode == "err" {
		return errors.New(MockReadMode)
	}
	return nil
}
