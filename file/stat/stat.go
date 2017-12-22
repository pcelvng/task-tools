package stat

import "hash"

func NewStat() *Stat {
	return &Stat{}
}

// Stat provides a common store for file statistics.
type Stat struct {
	// LineCount returns the current number of lines written to
	// the file. Should be safe to call at any time.
	// Calling LineCount after a call to Discard should return 0.
	LineCnt int64

	// ByteCount returns the total file size in bytes. ByteCount
	// should return 0 until after Finish is called.
	ByteCnt int64

	// Checksum should return the file checksum hash.
	// Can be called at any time but until the last line
	// is written will not represent the final checksum.
	CheckSum hash.Hash64

	// Path should return the expected final absolute file path.
	// The value should be the same regardless of calling
	// Close before Finish or having written any lines.
	//
	// The path should include any necessary file prefixes for
	// non-local files.
	Path string

	// TmpPath should return the current tmp file path as
	// an absolute path.
	//
	// If the writer is not writing to a tmp file then a
	// call to TmpPath should return an empty string.
	TmpPath string
}