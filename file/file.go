package file

import (
	"io"
	"net/url"

	"github.com/pcelvng/task-tools/file/local"
	"github.com/pcelvng/task-tools/file/nop"
	"github.com/pcelvng/task-tools/file/s3"
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

func NewStatsReader(pth string, opt *Options) (r StatsReadCloser, err error) {
	if opt == nil {
		opt = NewOptions()
	}
	var u *url.URL
	u, err = url.Parse(pth)
	if err != nil {
		return
	}

	switch u.Scheme {
	case "s3://":
		s3Opts := S3Options(*opt)
		r, err = s3.NewReader(pth, &s3Opts)
	case "nop://":
		r = nop.NewReader(pth)
	default:
		r, err = local.NewReader(pth)
	}

	return
}

func NewStatsWriter(pth string, opt *Options) (r StatsWriteCloser, err error) {
	if opt == nil {
		opt = NewOptions()
	}
	var u *url.URL
	u, err = url.Parse(pth)
	if err != nil {
		return
	}

	switch u.Scheme {
	case "s3://":
		s3Opts := S3Options(*opt)
		r, err = s3.NewWriter(pth, &s3Opts)
	case "nop://":
		r = nop.NewWriter(pth)
	default:
		localOpts := LocalOptions(*opt)
		r, err = local.NewWriter(pth, &localOpts)
	}

	return
}

func NewOptions() *Options {
	return &Options{}
}

// Options presents general options across all stats readers and
// writers.
type Options struct {
	AWSSecretKey string
	AWSAccessKey string

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

func S3Options(opt Options) s3.Options {
	s3Opts := s3.NewOptions()
	s3Opts.AccessKey = opt.AWSAccessKey
	s3Opts.SecretKey = opt.AWSSecretKey
	s3Opts.UseFileBuf = opt.UseFileBuf
	s3Opts.FileBufDir = opt.FileBufDir
	s3Opts.FileBufPrefix = opt.FileBufPrefix
	return *s3Opts
}

func LocalOptions(opt Options) local.Options {
	localOpts := local.NewOptions()
	localOpts.UseTmpFile = opt.UseFileBuf
	localOpts.TmpDir = opt.FileBufDir
	localOpts.TmpPrefix = opt.FileBufPrefix
	return *localOpts
}
