package local

func NewReader() *Reader {
	return &Reader{}
}

type Reader struct {

}

func (r *Reader) Read(p []byte) (n int, err error) { return 0, nil}

func (r *Reader) LineCount() int { return 0 }

func (r *Reader) Close() error { return nil }