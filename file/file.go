package file

import (
	"io"
	"context"
	"github.com/pcelvng/task-tools/file/stat"
)

// StatsReader is a special kind of reader that
// also provides some basic statistics like LineCount.
//
// Implementation should support reading uncompressed and compressed (gzip)
// files.
type StatsReader interface {
	// Close should be save to call at any time and should
	// clean up open connections and tmp files.
	io.Closer

	// ReadLine should return a whole line of bytes not including
	// the newline delimiter. When the end of the file is reached
	// should return the last line of bytes (if any) and an instance
	// of io.EOF for the error.
	//
	// A call to ReadLine after Close should return an error. (panic instead???)
	//
	// Should be safe to call concurrently.
	ReadLine() ([]byte, error)

	// Stats returns an instance of Stat.
	Stats() *stat.Stat
}

// StatsWriter is a special kind of writer that
// also provides some basic statistics like line count,
// file checksum, and total file size in bytes.
//
// Implementation should support writing uncompressed and compressed (gzip)
// files. For compressed files it should support streamed compression and
// delayed compression. Delayed compression means all compression happens after
// all the lines have been written so that better compression can be achieved.
//
// Implementation should support writing to an interim tmp file for delayed final
// writing. That way if the batch fails and the writer is closed before finishing
// a final existing destination file is not partially written (or removed on cleanup).
// And the result space is not corrupted. Also, writing to a tmp file and then
// copying to a final destination is a good practice, especially for remote
// destinations. Among the benefits include being able to compare the tmp checksum
// and final file checksum to make sure the file was copied correctly.
//
// But a tmp file needs to be optional in case one only wishes to append to an existing
// file or write directly to the final file for simplicity (especially for testing) or
// possibly reducing over-all task completion time which may or may not be longer using a
// tmp file.
type StatsWriter interface {
	// calling close before a call to finish should indicate aborting
	// the write and the writer should attempt to remove the partially
	// written file.
	io.Closer

	// WriteLine will write a line of bytes.
	// The user should not need to add the newline,
	// the implementation should do that for the user.
	// Ideally the implementation should not produce a trailing
	// newline at the end of the file.
	//
	// A call to WriteLine after Finish or Close should
	// panic.
	//
	// Should be safe to call concurrently and concurrent
	// calling should not corrupt the output.
	WriteLine([]byte) (int64, error)

	// Finish should finish up writing a file. That may mean
	// for example:
	// - gzipping the file (if not done as a stream)
	// - finalizing the byte count
	// - finalizing the checksum
	// - finalizing the line count
	// - doing file copy from a tmp file to the final file
	// destination. All implementations should support the
	// option to write to a temp file.
	// - removing the tmp file
	Finish() error

	// Stats returns an instance of Stat.
	Stats() *stat.Stat
}

type Copier interface {
	// Copy will copy a file from one location to another. The fromPath
	// and toPath strings can represent a file path from any supported file
	// location to any supported file location.
	//
	// Instead of opening up a file and reading and writing it line-by-line,
	// the Copy method will allow the file to be copied byte-for-byte. This
	// also means that if the file is compressed the copier will not need to
	// decompress the file.
	//
	// The newly created copy should have the same byte count and checksum.
	// Copying a file will not read from the file and so will not need to know the
	// line count.
	//
	// The returned context can be checked to know when the copy is complete.
	// If the copy needs to be prematurely cancelled then the caller can call
	// the returned cancel function.
	Copy(fromPath, toPath string) (context.Context, context.CancelFunc)
}