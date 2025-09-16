package file

import (
	"compress/gzip"
	"fmt"
	"io"
	"iter"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/local"
	"github.com/pcelvng/task-tools/file/minio"
	"github.com/pcelvng/task-tools/file/nop"
	"github.com/pcelvng/task-tools/file/stat"
)

// Reader is an io.ReadCloser that also provides
// file statistics along with a few additional methods.
type Reader interface {
	// ReadCloser should behave as defined in the io.Read interface.
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

	// Lines iterators through the file and return one line at a time
	//Lines() iter.Seq[[]byte]

	// Stats returns an instance of Stats.
	Stats() stat.Stats
}

// Writer is a io.WriteCloser that also provides
// file statistics along with a few additional methods.
type Writer interface {
	// WriteCloser should behave as defined in io.Writer so that it
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

// bufOptions converts a full file.Options to a buf.Options used for the buffer.
// this avoids circular imports
func bufOptions(opt Options) buf.Options {
	return buf.Options{
		CompressLevel: compressionLookup(opt.CompressionLevel),
		UseFileBuf:    opt.UseFileBuf,
		FileBufDir:    opt.FileBufDir,
		FileBufPrefix: opt.FileBufPrefix,
	}
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

	mOpt := minio.Option{AccessKey: opt.AccessKey, SecretKey: opt.SecretKey, Secure: true}
	switch u.Scheme {
	case "s3":
		mOpt.Host = minio.S3Host
		return minio.NewReader(pth, mOpt)
	case "gcs", "gs":
		mOpt.Host = minio.GSHost
		return minio.NewReader(pth, mOpt)
	case "mc", "minio":
		mOpt.Host = u.Host
		mOpt.Secure = false
		return minio.NewReader(pth, mOpt)
	case "mcs":
		mOpt.Host = u.Host
		mOpt.Secure = true
		return minio.NewReader(pth, mOpt)
	case "nop":
		return nop.NewReader(pth)
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

	if pth == "" {
		return nop.NewWriter(pth)

	}

	u, err := url.Parse(pth)
	if err != nil {
		return
	}
	bufOpts := bufOptions(*opt)
	mOpt := minio.Option{AccessKey: opt.AccessKey, SecretKey: opt.SecretKey, Secure: true}
	switch u.Scheme {
	case "s3":
		mOpt.Host = minio.S3Host
		return minio.NewWriter(pth, mOpt, &bufOpts)
	case "gcs", "gs":
		mOpt.Host = minio.GSHost
		return minio.NewWriter(pth, mOpt, &bufOpts)
	case "mc", "minio":
		mOpt.Host = u.Host
		mOpt.Secure = false
		return minio.NewWriter(pth, mOpt, &bufOpts)
	case "mcs":
		mOpt.Host = u.Host
		mOpt.Secure = true
		return minio.NewWriter(pth, mOpt, &bufOpts)
	case "nop":
		w, err = nop.NewWriter(pth)
	case "local":
		fallthrough
	default:
		w, err = local.NewWriter(pth, &bufOpts)
	}

	return w, err
}

type Iterator struct {
	err     error
	isValid bool
	reader  Reader
}

func NewIterator(path string, opts *Options) *Iterator {
	r, err := NewReader(path, opts)
	return &Iterator{
		err:     err,
		isValid: err == nil,
		reader:  r,
	}
}

// Lines allow ranges through file return a []byte for each line in the file
// range will stop for any error conditions include on opening the file.
func (i *Iterator) Lines() iter.Seq[[]byte] {
	// only iterator if the reader is properly set.
	if i.isValid {
		return func(yield func([]byte) bool) {
			var err error
			// close reader and properly record error
			defer func() {
				i.err = err
				closeErr := i.reader.Close()
				if closeErr != nil && i.err != nil {
					i.err = fmt.Errorf("%w + close-err:%v", err, closeErr)
				} else if closeErr != nil {
					i.err = closeErr
				}
			}()
			var ln []byte
			for ln, err = i.reader.ReadLine(); err == nil; ln, err = i.reader.ReadLine() {
				if !yield(ln) {
					return
				}
			}
			if err == io.EOF {
				err = nil
				yield(ln)
			}
		}
	}
	return func(yield func([]byte) bool) {}
}

// Stats of the file that was read
func (i *Iterator) Stats() stat.Stats {
	if i.isValid {
		return i.reader.Stats()
	}
	return stat.Stats{}
}

// Error caused by reading the file. This could be from
// 1. Open the file
// 2. Reading/Retrieving a line
// 3. Closing the file
func (i *Iterator) Error() error {
	return i.err
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

	u, err := url.Parse(pthDir)
	if err != nil {
		return nil, err
	}
	mOpt := minio.Option{AccessKey: opt.AccessKey, SecretKey: opt.SecretKey, Secure: true}
	switch u.Scheme {
	case "s3":
		mOpt.Host = minio.S3Host
		return minio.ListFiles(pthDir, mOpt)
	case "gs":
		mOpt.Host = minio.GSHost
		return minio.ListFiles(pthDir, mOpt)
	case "mc", "minio":
		mOpt.Host = u.Host
		mOpt.Secure = false
		return minio.ListFiles(pthDir, mOpt)
	case "mcs":
		mOpt.Host = u.Host
		mOpt.Secure = true
		return minio.ListFiles(pthDir, mOpt)
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
	u, err := url.Parse(path)
	if err != nil {
		return stat.Stats{}, err
	}
	mOpt := minio.Option{AccessKey: opt.AccessKey, SecretKey: opt.SecretKey, Secure: true}
	switch u.Scheme {
	case "s3":
		mOpt.Host = minio.S3Host
		return minio.Stat(path, mOpt)
	case "gs":
		mOpt.Host = minio.GSHost
		return minio.Stat(path, mOpt)
	case "mc", "minio":
		mOpt.Host = u.Host
		mOpt.Secure = false
		return minio.Stat(path, mOpt)
	case "mcs":
		mOpt.Host = u.Host
		mOpt.Secure = true
		return minio.Stat(path, mOpt)
	case "nop":
		return nop.Stat(path)
	}
	return local.Stat(path)
}

// Glob will match to files and folder
//
// Supports the same globing patterns as provided in *nix
// terminals.
//
// Globing in directories is supported.
// ie - s3://bucket/path/*/files.txt will work
// s3://bucket/path/dir[0-5]*/*.txt will work
// but s3://bucket/path/to/*.txt will work.
func Glob(pth string, opt *Options) ([]stat.Stats, error) {
	if opt == nil {
		opt = NewOptions()
	}
	pthDir, pattern := path.Split(pth)
	folders := []string{pthDir}
	// check pthDir for pattern matches
	if strings.ContainsAny(pthDir, "[]*?") {
		f, err := matchFolder(pthDir, opt)
		if err != nil {
			return nil, err
		}
		folders = make([]string, len(f))
		for i, v := range f {
			folders[i] = v.Path
		}
	}
	allSts := make([]stat.Stats, 0)

	// get all files in dir
	for _, f := range folders {
		sts, err := List(f, opt)
		if err != nil {
			return nil, err
		}
		allSts = append(allSts, sts...)
	}

	// filter out files that don't match the glob pattern
	glbSts := make([]stat.Stats, 0)
	for _, sts := range allSts {
		_, fName := path.Split(sts.Path)
		if sts.IsDir {
			continue
		}
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

// matchFolder resolves and returns directories that match a globbed path prefix.
//
// Behavior:
//   - Splits the provided path into a parent directory and a final segment.
//   - Recursively expands any glob/meta characters ([], *, ?) that appear in the
//     parent directory portion, producing a concrete list of parent directories.
//   - For each resolved parent, lists its entries and matches only subdirectories
//     against the final segment. The final segment can be a literal or a pattern;
//     literal is compared by equality, patterns use filepath.Match.
//   - Returns the directories that match the final segment as stat.Stats entries.
//
// This function is used by Glob to support nested directory globs such as:
//
//	"./a/*/b[1-3]/*/file.txt"
//
// ensuring trailing directory segments are preserved and matched correctly.
func matchFolder(pth string, opt *Options) (folders []stat.Stats, err error) {
	// Resolve patterns in the parent directory first, then match the final segment
	parentDir, segment := path.Split(strings.TrimRight(pth, "/"))

	// Expand any patterns in the parent directory by recursion
	parentPaths := []string{parentDir}
	if strings.ContainsAny(parentDir, "[]*?") {
		sts, err := matchFolder(strings.TrimRight(parentDir, "/"), opt)
		if err != nil {
			return nil, err
		}
		parentPaths = make([]string, 0, len(sts))
		for _, f := range sts {
			parentPaths = append(parentPaths, f.Path)
		}
	}

	for _, p := range parentPaths {
		sts, err := List(p, opt)
		if err != nil {
			return nil, err
		}
		for _, f := range sts {
			if !f.IsDir {
				continue
			}
			_, fName := path.Split(strings.TrimRight(f.Path, "/"))
			isMatch := false
			if strings.ContainsAny(segment, "[]*?") {
				if m, err := filepath.Match(segment, fName); err != nil {
					return nil, err
				} else if m {
					isMatch = true
				}
			} else {
				isMatch = (fName == segment)
			}
			if isMatch {
				folders = append(folders, f)
			}
		}
	}

	return folders, nil
}
