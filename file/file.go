package file

import (
	"io"
	"hash"
)

type StatsReadCloser interface {
	io.Closer

	// ReadLine should return a whole line of bytes not including
	// the newline delimiter. When the end of the file is reached
	// should return the last line of bytes (if any) and an instance
	// of io.EOF for the error.
	ReadLine() ([]byte, error)

	// Do this???
	ReadStream() (chan []byte, error)

	// Do this??? - some other kind of raw bytes reader for quick copying
	ReadFull() ([]byte, error)

	// LineCount returns the current number of lines read from
	// the file. Should be safe to call at any time.
	// Calling LineCount after a call to Discard should return 0.
	LineCount() int
}

type StatsWriteCloser interface {
	io.Closer

	// WriteLine will write a whole line of bytes.
	// The implementation should add the newline.
	WriteLine([]byte) (int, error)

	Write([]byte) (int, error)

	// Hash should return the file checksum hash.
	// Can be called at any time but until the last line
	// is written will not represent the final checksum.
	Hash() hash.Hash64

	// Discard should remove the file. If
	// the file is unable to be removed
	// then an error should be returned.
	Discard() error

	// LineCount returns the current number of lines written to
	// the file. Should be safe to call at any time.
	// Calling LineCount after a call to Discard should return 0.
	LineCount() int
}

