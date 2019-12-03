package file

import (
	"compress/gzip"
	"context"
	"io"
	"net/url"
	"path"
	"path/filepath"

	"github.com/pcelvng/task-tools/file/gs"
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

// NewOptions
func NewOptions() *Options {
	return &Options{
		CompressionLevel: "speed",
	}
}

// Options presents general options across all stats readers and
// writers.
type Options struct {
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`

	CompressionLevel string `toml:"file_compression" commented:"true" comment:"gzip compression level (speed|size|default)"`

	// UseFileBuf specifies to use a tmp file for the delayed writing.
	// Can optionally also specify the tmp directory and tmp name
	// prefix.
	UseFileBuf bool `toml:"use_file_buf" commented:"true" comment:"set as 'true' if files are too big to buffer in memory"`

	// FileBufDir optionally specifies the temp directory. If not specified then
	// the os default temp dir is used.
	FileBufDir string `toml:"file_buf_dir" commented:"true" comment:"temp file directory if buffering files to disk (default is the os temp directory, note: app user must have access to this directory)"`

	// FileBufPrefix optionally specifies the temp file prefix.
	// The full tmp file name is randomly generated and guaranteed
	// not to conflict with existing files. A prefix can help one find
	// the tmp file.
	//
	// In an effort to encourage fewer application configuration options
	// this value not made available to a toml config file and the default
	// is set to 'task-type_' by the application bootstrapper.
	//
	// If no prefix is provided then the temp file name is just a random
	// unique number.
	FileBufPrefix     string `toml:"-"` // default is usually 'task-type_'
	FileBufKeepFailed bool   `toml:"file_buf_keep_failed" commented:"true" comment:"keep the local buffer file on a upload failure"`
}

func compressionLookup(s string) int {
	switch s {
	case "speed":
		return gzip.BestSpeed
	case "size":
		return gzip.BestCompression
	default:
		return gzip.DefaultCompression
	}
}

func s3Options(opt Options) s3.Options {
	s3Opts := s3.NewOptions()
	s3Opts.CompressLevel = compressionLookup(opt.CompressionLevel)
	s3Opts.UseFileBuf = opt.UseFileBuf
	s3Opts.FileBufDir = opt.FileBufDir
	s3Opts.FileBufPrefix = opt.FileBufPrefix
	s3Opts.KeepFailed = opt.FileBufKeepFailed
	return *s3Opts
}

func gcsOptions(opt Options) gs.Options {
	gcsOpts := gs.NewOptions()
	gcsOpts.CompressLevel = compressionLookup(opt.CompressionLevel)
	gcsOpts.UseFileBuf = opt.UseFileBuf
	gcsOpts.FileBufDir = opt.FileBufDir
	gcsOpts.FileBufPrefix = opt.FileBufPrefix
	gcsOpts.KeepFailed = opt.FileBufKeepFailed
	return *gcsOpts
}

func localOptions(opt Options) local.Options {
	localOpts := local.NewOptions()
	localOpts.CompressLevel = compressionLookup(opt.CompressionLevel)
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
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		r, err = s3.NewReader(pth, accessKey, secretKey)
	case "gcs", "gs":
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		r, err = gs.NewReader(pth, accessKey, secretKey)
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
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		s3Opts := s3Options(*opt)
		w, err = s3.NewWriter(pth, accessKey, secretKey, &s3Opts)
	case "gcs", "gs":
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		gcsOpts := gcsOptions(*opt)
		w, err = gs.NewWriter(pth, accessKey, secretKey, &gcsOpts)
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
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		return s3.ListFiles(pthDir, accessKey, secretKey)
	case "gs":
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		return gs.ListFiles(pthDir, accessKey, secretKey)
	case "nop":
		return nop.ListFiles(pthDir)
	}
	return local.ListFiles(pthDir)
}

// Stat returns a summary stats of a file or directory.
// It can be used to verify read permissions
func Stat(path string, opt *Options) (stat.Stats, error) {
	if opt == nil {
		opt = NewOptions()
	}
	switch parseScheme(path) {
	case "s3":
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		return s3.Stat(path, accessKey, secretKey)
	case "gs":
		accessKey := opt.AccessKey
		secretKey := opt.SecretKey
		return gs.Stat(path, accessKey, secretKey)
	}
	return local.Stat(path)
}

// Glob will only match to files and will
// not match recursively. Only files directly in pthDir
// are candidates for matching.
//
// Supports the same globing patterns as provided in *nix
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

// ReadLines is a high-level utility that will read all the lines of a reader and call
// f when the number of bytes is > 0. err will never be EOF and if cncl == true
// then err will be nil.
func ReadLines(ctx context.Context, r Reader, f func(ln []byte) error) (err error, cncl bool) {
	for ctx.Err() == nil {
		// read
		ln, err := r.ReadLine()
		if err != nil && err != io.EOF {
			return err, false
		}

		// add record
		if len(ln) > 0 {
			if err = f(ln); err != nil {
				return err, false
			}
		}

		if err == io.EOF {
			break
		}
	}

	// check ctx
	if ctx.Err() != nil {
		return nil, true
	}

	return nil, false
}
