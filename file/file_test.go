package file

import (
	"os"
	"testing"

	"log"

	"net/url"
	"strings"

	"fmt"

	"github.com/hydronica/trial"
	minio "github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/s3"
)

var (
	// minio test server credentials
	//
	// see:
	// https://docs.minio.io/docs/golang-client-api-reference
	testEndpoint = "play.minio.io:9000"
	testBucket   = "task-tools-test"
	testS3Client *minio.Client

	wd   string
	opts = Options{
		AccessKey: "Q3AM3UQ867SPQQA43P2F",
		SecretKey: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
	}
)

func TestMain(m *testing.M) {

	// setup local files test
	wd, _ = os.Getwd()

	// setup remote (minio/s3/gcs) test
	s3.StoreHost = testEndpoint // set test endpoint

	// s3 client
	var err error
	testS3Client, err = minio.New(s3.StoreHost, opts.AccessKey, opts.SecretKey, true)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// bucket
	err = createBucket(testBucket)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
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
	for _, pth := range pths {
		createFile("./"+pth, &opts)                                   // local
		createFile(fmt.Sprintf("s3://%s/%s", testBucket, pth), &opts) // remote
	}

	code := m.Run()

	// cleanup
	os.RemoveAll("./test/")
	rmS3Bucket(testBucket)
	os.Exit(code)
}

func TestGlob_Local(t *testing.T) {
	fn := func(input trial.Input) (interface{}, error) {
		sts, err := Glob(input.String(), nil)
		files := make([]string, len(sts))
		for i := 0; i < len(sts); i++ {
			files[i] = strings.Replace(sts[i].Path, wd, ".", -1)
		}
		return files, err
	}
	cases := trial.Cases{
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
	}
	trial.New(fn, cases).SubTest(t)

}

func TestGlob_S3(t *testing.T) {
	path := "s3://" + testBucket
	fn := func(input trial.Input) (interface{}, error) {
		sts, err := Glob(input.String(), &opts)
		files := make([]string, len(sts))
		for i := 0; i < len(sts); i++ {
			files[i] = sts[i].Path
		}
		return files, err
	}
	cases := trial.Cases{
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
	trial.New(fn, cases).SubTest(t)
}

func createFile(pth string, opt *Options) {
	w, _ := NewWriter(pth, opt)
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
}

func createBucket(bckt string) error {
	exists, err := testS3Client.BucketExists(bckt)
	if err != nil || exists {
		return err
	}

	return testS3Client.MakeBucket(bckt, "us-east-1")
}

func rmS3Bucket(bckt string) error {
	return testS3Client.RemoveBucket(bckt)
}

func rmS3File(pth string) error {
	bckt, objPth := parseS3Pth(pth)
	return testS3Client.RemoveObject(bckt, objPth)
}

func parseS3Pth(pth string) (bucket, objPth string) {
	// err is not possible since it's not via a request.
	pPth, _ := url.Parse(pth)
	bucket = pPth.Host
	objPth = strings.TrimLeft(pPth.Path, "/")
	return bucket, objPth
}
