package file

import (
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/hydronica/trial"
	"github.com/minio/minio-go"
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
	testClient, err = minio.New(testEndpoint, opts.AccessKey, opts.SecretKey, true)
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
	os.MkdirAll("./test/f1", 0750)
	os.MkdirAll("./test/f3", 0750)
	os.MkdirAll("./test/f5", 0750)
	for _, pth := range pths {
		if err := createFile("./"+pth, &opts); err != nil {
			log.Fatal(err)
		} // local
		if err := createFile(fmt.Sprintf("mc://%s/%s/%s", testEndpoint, testBucket, pth), &opts); err != nil { // remote
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
	path := "mc://" + testEndpoint + "/" + testBucket
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
	// TODO: test minio glob m3:localhost:9000/*/*.txt
	trial.New(fn, cases).SubTest(t)
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
	exists, err := testClient.BucketExists(bckt)
	if err != nil || exists {
		return err
	}

	return testClient.MakeBucket(bckt, "us-east-1")
}

func rmBucket(bckt string) error {
	return testClient.RemoveBucket(bckt)
}
