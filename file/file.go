package file

import (
	"io"

	"github.com/pcelvng/task-tools/file/stat"
)

// StatsReadCloser is an io.ReadCloser that also provides
// file statistics along with a few additional methods.
type StatsReadCloser interface {
	// Read should behave as defined in the io.Read interface.
	// In this way we can take advantage of all standard library
	// methods that rely on Read such as copy.
	//
	// Close should do any necessary standard closing but also
	// do final syncing/flushing/cleanup esp when reading
	// from a remote source.
	io.ReadCloser

	// ReadLine should return a whole line of bytes not including
	// the newline delimiter. When the end of the file is reached, it
	// should return the last line of bytes (if any) and an instance
	// of io.EOF for the error.
	//
	// A call to ReadLine after Close has undefined behavior.
	ReadLine() ([]byte, error)

	// Stats returns an instance of Stat.
	Stats() stat.Stat
}

// StatsWriteCloser is a io.WriteCloser that also provides
// file statistics along with a few additional methods.
type StatsWriteCloser interface {
	// Write should behave as defined in io.Writer so that it
	// is compatible with standard library tooling such as
	// io.Copy. Additionally concurrent calls to Write should
	// be safe and not corrupt the output. Order may
	// not be guaranteed.
	//
	// Close should do any necessary standard closing but also
	// do final copying/syncing/flushing to local and remote
	// locations. Should also gather final stats for a call
	// to the Stats method.
	io.WriteCloser

	// WriteLine will write a line of bytes.
	// The user should not need to add the newline,
	// the implementation should do that for the user.
	//
	// Should be safe to call concurrently and concurrent
	// calling should not corrupt the output. Concurrent calling
	// does not guarantee order but one record will not partially
	// over-write another.
	WriteLine([]byte) error

	// Stats returns the file stats. Safe to call any time.
	Stats() stat.Stat

	// Abort can be called anytime before or during a call
	// to Close. Will block until abort cleanup is complete.
	Abort() error
}
