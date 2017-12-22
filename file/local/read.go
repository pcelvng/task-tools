package local


func NewLocal() *Local {
	return &Local{}
}

type ReaderConfig struct {
	Path string
	TmpDir string
}

type Local struct {

}

func (r *Local) ReadLine() ([]byte, error) { return nil, nil}

func (r *Local) LineCount() int64 { return 0 }

func (r *Local) Path() string { return "" }

func (r *Local) TmpPath() string { return "" }

func (r *Local) Close() error { return nil }

