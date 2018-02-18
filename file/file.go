package file

import (
	"io"
	"net/url"

	"path/filepath"

	"path"

	"github.com/pcelvng/task-tools/file/local"
	"github.com/pcelvng/task-tools/file/nop"
	"github.com/pcelvng/task-tools/file/s3"
	"github.com/pcelvng/task-tools/file/stat"
)

// Reader is an io.ReadCloser that also provides
// file statistics along with a few additional methods.
type Reader interface {
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

	// Stats returns an instance of Stats.
	Stats() stat.Stats
}

// Writer is a io.WriteCloser that also provides
// file statistics along with a few additional methods.
type Writer interface {
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
	Stats() stat.Stats

	// Abort can be called anytime before or during a call
	// to Close. Will block until abort cleanup is complete.
	Abort() error
}

func NewOptions() *Options {
	return &Options{}
}

// Options presents general options across all stats readers and
// writers.
type Options struct {
	AWSAccessKey string
	AWSSecretKey string

	// UseFileBuf specifies to use a tmp file for the delayed writing.
	// Can optionally also specify the tmp directory and tmp name
	// prefix.
	UseFileBuf bool

	// FileBufDir optionally specifies the temp directory. If not specified then
	// the os default temp dir is used.
	FileBufDir string

	// FileBufPrefix optionally specifies the temp file prefix.
	// The full tmp file name is randomly generated and guaranteed
	// not to conflict with existing files. A prefix can help one find
	// the tmp file.
	FileBufPrefix string
}

func s3Options(opt Options) s3.Options {
	s3Opts := s3.NewOptions()
	s3Opts.UseFileBuf = opt.UseFileBuf
	s3Opts.FileBufDir = opt.FileBufDir
	s3Opts.FileBufPrefix = opt.FileBufPrefix
	return *s3Opts
}

func localOptions(opt Options) local.Options {
	localOpts := local.NewOptions()
	localOpts.UseFileBuf = opt.UseFileBuf
	localOpts.FileBufDir = opt.FileBufDir
	localOpts.FileBufPrefix = opt.FileBufPrefix
	return *localOpts
}

func NewReader(pth string, opt *Options) (r Reader, err error) {
	if opt == nil {
		opt = NewOptions()
	}
	var u *url.URL
	u, err = url.Parse(pth)
	if err != nil {
		return
	}

	switch u.Scheme {
	case "s3":
		accessKey := opt.AWSAccessKey
		secretKey := opt.AWSSecretKey
		r, err = s3.NewReader(pth, accessKey, secretKey)
	case "nop":
		r, err = nop.NewReader(pth)
	case "local":
		fallthrough
	default:
		r, err = local.NewReader(pth)
	}

	return
}

func NewWriter(pth string, opt *Options) (w Writer, err error) {
	if opt == nil {
		opt = NewOptions()
	}

	switch parseScheme(pth) {
	case "s3":
		accessKey := opt.AWSAccessKey
		secretKey := opt.AWSSecretKey
		s3Opts := s3Options(*opt)
		w, err = s3.NewWriter(pth, accessKey, secretKey, &s3Opts)
	case "nop":
		w, err = nop.NewWriter(pth)
	case "local":
		fallthrough
	default:
		localOpts := localOptions(*opt)
		w, err = local.NewWriter(pth, &localOpts)
	}

	return w, err
}

// List is a generic List function that will call the
// correct type of implementation based on the file schema, aka
// 's3://'. If there is no schema or if the schema is 'local://'
// then the local file List will be called.
//
// pthDir is expected to be a dir.
func List(pthDir string, opt *Options) ([]stat.Stats, error) {
	if opt == nil {
		opt = NewOptions()
	}

	fileType := parseScheme(pthDir)
	switch fileType {
	case "s3":
		accessKey := opt.AWSAccessKey
		secretKey := opt.AWSSecretKey
		return s3.ListFiles(pthDir, accessKey, secretKey)
	case "nop":
		return nop.ListFiles(pthDir)
	}
	return local.ListFiles(pthDir)
}

// Glob will only match to files and will
// not match recursively. Only files directly in pthDir
// are candidates for matching.
//
// Supports the same globbing patterns as provided in *nix
// terminals.
//
// Globing in directories is not supported.
// ie - s3://bucket/path/*/files.txt will not work
// but s3://bucket/path/to/*.txt will work.
func Glob(pth string, opt *Options) ([]stat.Stats, error) {
	if opt == nil {
		opt = NewOptions()
	}
	pthDir, pattern := path.Split(pth)

	// get all files in dir
	allSts, err := List(pthDir, opt)
	if err != nil {
		return nil, err
	}

	// filter out files that don't match the glob pattern
	glbSts := make([]stat.Stats, 0)
	for _, sts := range allSts {
		_, fName := path.Split(sts.Path)
		isMatch, err := filepath.Match(pattern, fName)
		if err != nil {
			return nil, err
		}

		if isMatch {
			glbSts = append(glbSts, sts)
		}
	}

	return glbSts, nil
}

// parseScheme will return the pth scheme (if exists).
// If there is no scheme then an empty string is returned.
func parseScheme(pth string) string {
	u, err := url.Parse(pth)
	if err != nil {
		return ""
	}

	return u.Scheme
}
