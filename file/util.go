package file

import "context"

func NewCopier(r StatsReadCloser, w StatsWriteCloser) *Copier {
	return &Copier{
		r: r,
		w: w,
	}
}

type Copier struct {
	r StatsReadCloser
	w StatsWriteCloser
}

// Copy will copy the contents of the reader to the writer.
// Copy is non-blocking so that the copy can be cancelled.
// Listen on Context.Done() to know when the copy is complete.
//
// More than one call to Copy will result in a panic.
func (c *Copier) Copy() context.Context {
	return context.Background()
}

// Cancel will cancel the copy. The copier will attempt to
// cleanup the write file by removing or truncating the write
// file. To know when the copier is done with cleanup listen to
// the Context.Done() channel.
//
// Cancel is safe to call more than once but subsequent calls don't
// do anything. If it's called again. The same context instance is returned.
func (c *Copier) Cancel() context.Context {
	return context.Background()
}

// Err will only contain an error if there was a problem
// either copying the file or cancelling a copy.
// The user should check Err after the Copy or Cancel done signals
// are sent. Err can be called multiple times and get the same
// err.
func (c *Copier) Err() error {
	return nil
}