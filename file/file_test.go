package file

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hydronica/trial"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/pcelvng/task-tools/file/stat"
)

var (
	// minio test server credentials
	//
	// see:
	// https://docs.minio.io/docs/golang-client-api-reference
	testEndpoint = "play.minio.io:9000"
	testBucket   = "task-tools-test"
	testClient   *minio.Client

	wd   string
	opts = Options{
		AccessKey: "Q3AM3UQ867SPQQA43P2F",
		SecretKey: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
	}
)

func TestMain(m *testing.M) {
	log.SetFlags(log.Lshortfile)

	// setup local files test
	wd, _ = os.Getwd()

	// setup remote (minio/s3/gcs) test

	// s3 client
	var err error
	testClient, err = minio.New(testEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.AccessKey, opts.SecretKey, ""),
		Secure: true,
	})
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// bucket
	err = createBucket(testBucket)
	if err != nil {
		log.Println(err)
	}

	// create files
	pths := []string{
		"test/file-1.txt",
		"test/file2.txt",
		"test/file3.gz",
		"test/f1/file4.gz",
		"test/f3/file5.txt",
		"test/f5/file-6.txt",
	}
	os.MkdirAll("./test/f1", 0750)
	os.MkdirAll("./test/f3", 0750)
	os.MkdirAll("./test/f5", 0750)
	for _, pth := range pths {
		if err := createFile("./"+pth, &opts); err != nil {
			log.Fatal(err)
		} // local
		if err := createFile(fmt.Sprintf("mcs://%s/%s/%s", testEndpoint, testBucket, pth), &opts); err != nil { // remote
			log.Fatal(err)
		}

	}

	code := m.Run()

	// cleanup
	os.RemoveAll("./test/")
	rmBucket(testBucket)
	os.Exit(code)
}

func TestGlob_Local(t *testing.T) {
	fn := func(input string) ([]string, error) {
		sts, err := Glob(input, nil)
		files := make([]string, len(sts))
		for i := 0; i < len(sts); i++ {
			files[i] = strings.Replace(sts[i].Path, wd, ".", -1)
		}
		return files, err
	}
	cases := trial.Cases[string, []string]{
		"star.txt": {
			Input:    "./test/*.txt",
			Expected: []string{"./test/file-1.txt", "./test/file2.txt"},
		},
		"file?.txt": {
			Input:    "./test/file?.txt",
			Expected: []string{"./test/file2.txt"},
		},
		"file?.star": {
			Input:    "./test/file?.*",
			Expected: []string{"./test/file2.txt", "./test/file3.gz"},
		},
		"folders": {
			Input:    "./test/*/*",
			Expected: []string{"./test/f1/file4.gz", "./test/f3/file5.txt", "./test/f5/file-6.txt"},
		},
		"range": {
			Input:    "test/f[1-3]/*",
			Expected: []string{"./test/f1/file4.gz", "./test/f3/file5.txt"},
		},
		"folder/star.txt": {
			Input:    "test/*/*.txt",
			Expected: []string{"./test/f3/file5.txt", "./test/f5/file-6.txt"},
		},
		"file": {
			Input:    "test/file2.txt",
			Expected: []string{"./test/file2.txt"},
		},
		"nop/file": {
			Input:    "nop://file.txt", //NOTE nop is hard-coded to return file.txt
			Expected: []string{"nop://file.txt"},
		},
	}
	trial.New(fn, cases).SubTest(t)
}

func TestGlob_Minio(t *testing.T) {
	path := "mcs://" + testEndpoint + "/" + testBucket
	fn := func(input string) ([]string, error) {
		sts, err := Glob(input, &opts)
		files := make([]string, len(sts))
		for i := 0; i < len(sts); i++ {
			files[i] = sts[i].Path
		}
		return files, err
	}
	cases := trial.Cases[string, []string]{
		"star.txt": {
			Input:    path + "/test/*.txt",
			Expected: []string{path + "/test/file-1.txt", path + "/test/file2.txt"},
		},
		"file?.txt": {
			Input:    path + "/test/file?.txt",
			Expected: []string{path + "/test/file2.txt"},
		},
		"file?.star": {
			Input:    path + "/test/file?.*",
			Expected: []string{path + "/test/file2.txt", path + "/test/file3.gz"},
		},
		"folders": {
			Input:    path + "/test/*/*",
			Expected: []string{path + "/test/f1/file4.gz", path + "/test/f3/file5.txt", path + "/test/f5/file-6.txt"},
		},
		"range": {
			Input:    path + "/test/f[1-3]/*",
			Expected: []string{path + "/test/f1/file4.gz", path + "/test/f3/file5.txt"},
		},
		"folder/star.txt": {
			Input:    path + "/test/*/*.txt",
			Expected: []string{path + "/test/f3/file5.txt", path + "/test/f5/file-6.txt"},
		},
	}
	// TODO: test minio glob m3:localhost:9000/*/*.txt
	trial.New(fn, cases).SubTest(t)
}

func TestReadFile(t *testing.T) {
	var data string
	for l := range Lines("../internal/test/nop.sql", nil, nil) {
		data += string(l)
	}
	fmt.Println(data)
}

func TestIterStruct(t *testing.T) {
	fn := func(path string) (stat.Stats, error) {
		it := Iterator(path, nil)
		for _ = range it.Range() {
		}
		return it.Stats(), it.Error()
	}
	cases := trial.Cases[string, stat.Stats]{
		"full file": {
			Input: "../internal/test/nop.sql",
			Expected: stat.Stats{
				LineCnt: 1,
				ByteCnt: 25,
				Size:    25,
			},
		},
		"init_err": {
			Input:       "nop://init_err",
			ExpectedErr: errors.New("init_err"),
		},
		"read_err": {
			Input:       "nop://readline_err",
			ExpectedErr: errors.New("readline_err"),
		},
		"close_err": {
			Input:       "nop://close_err",
			ExpectedErr: errors.New("close_err"),
		},
	}

	trial.New(fn, cases).
		Timeout(time.Second).
		Comparer(trial.EqualOpt(trial.IgnoreFields("Checksum", "Created", "Path"))).
		SubTest(t)
}

func TestIterator(t *testing.T) {
	fn := func(path string) (*stat.Stats, error) {
		it, sts := LineErr(path, nil)
		for _ = range it {
		}
		return sts, sts.Error
	}
	cases := trial.Cases[string, *stat.Stats]{
		"full file": {
			Input: "../internal/test/nop.sql",
			Expected: &stat.Stats{
				LineCnt: 1,
				ByteCnt: 25,
				Size:    25,
			},
		},
		"init_err": {
			Input:       "nop://init_err",
			ExpectedErr: errors.New("init_err"),
		},
		"read_err": {
			Input:       "nop://readline_err",
			ExpectedErr: errors.New("readline_err"),
		},
		"close_err": {
			Input:       "nop://close_err",
			ExpectedErr: errors.New("close_err"),
		},
	}

	trial.New(fn, cases).
		Timeout(time.Second).
		Comparer(trial.EqualOpt(trial.IgnoreFields("Checksum", "Created", "Path"))).
		SubTest(t)
}

func createFile(pth string, opt *Options) error {
	w, err := NewWriter(pth, opt)
	if err != nil {
		return err
	}

	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
	return nil
}

func createBucket(bckt string) error {
	exists, err := testClient.BucketExists(context.Background(), bckt)
	if err != nil || exists {
		return err
	}

	return testClient.MakeBucket(context.Background(), bckt, minio.MakeBucketOptions{})
}

func rmBucket(bckt string) error {
	return testClient.RemoveBucket(context.Background(), bckt)
}
