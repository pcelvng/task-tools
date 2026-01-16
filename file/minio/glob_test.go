package minio_test

import (
	"os"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task-tools/file"
)

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func TestGlob(t *testing.T) {
	endpoint := getEnv("MINIO_ENDPOINT", "localhost:9000")
	accessKey := getEnv("MINIO_ACCESS_KEY", "minioadmin")
	secretKey := getEnv("MINIO_SECRET_KEY", "minioadmin")
	testBucket := "task-tools-test"

	path := "mc://" + endpoint + "/" + testBucket
	opts := &file.Options{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	fn := func(input string) ([]string, error) {
		sts, err := file.Glob(input, opts)
		files := make([]string, len(sts))
		for i := 0; i < len(sts); i++ {
			files[i] = sts[i].Path
		}
		return files, err
	}
	cases := trial.Cases[string, []string]{
		"star.txt": {
			Input:    path + "/glob/*.txt",
			Expected: []string{path + "/glob/file-1.txt", path + "/glob/file2.txt"},
		},
		"file?.txt": {
			Input:    path + "/glob/file?.txt",
			Expected: []string{path + "/glob/file2.txt"},
		},
		"file?.star": {
			Input:    path + "/glob/file?.*",
			Expected: []string{path + "/glob/file2.txt", path + "/glob/file3.gz"},
		},
		"folders": {
			Input:    path + "/glob/*/*",
			Expected: []string{path + "/glob/f1/file4.gz", path + "/glob/f3/file5.txt", path + "/glob/f5/file-6.txt"},
		},
		"range": {
			Input:    path + "/glob/f[1-3]/*",
			Expected: []string{path + "/glob/f1/file4.gz", path + "/glob/f3/file5.txt"},
		},
		"folder/star.txt": {
			Input:    path + "/glob/*/*.txt",
			Expected: []string{path + "/glob/f3/file5.txt", path + "/glob/f5/file-6.txt"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}
