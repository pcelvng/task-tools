package s3

type Options struct {
	SecretToken string
	AccessToken string

	// UseTmpFile specifies to use a tmp file for the delayed writing.
	// Can optionally also specify the tmp directory and tmp name
	// prefix.
	UseTmpFile bool

	// TmpDir optionally specifies the temp directory. If not specified then
	// the os default temp dir is used.
	TmpDir string

	// TmpPrefix optionally specifies the temp file prefix.
	// The full tmp file name is randomly generated and guaranteed
	// not to conflict with existing files. A prefix can help one find
	// the tmp file.
	TmpPrefix string
}
