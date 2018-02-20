package file

import (
	"os"
	"testing"

	"log"

	"net/url"
	"strings"

	"path"

	"fmt"

	minio "github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/s3"
)

var (
	// minio test server credentials
	//
	// see:
	// https://docs.minio.io/docs/golang-client-api-reference
	testEndpoint  = "play.minio.io:9000"
	testAccessKey = "Q3AM3UQ867SPQQA43P2F"
	testSecretKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	testBucket    = "task-tools-test"
	testS3Client  *minio.Client
)

func TestGlob_Local(t *testing.T) {
	// create files
	pths := []string{
		"./test/file-1.txt",
		"./test/file2.txt",
		"./test/file3.gz",
	}
	for _, pth := range pths {
		createFile(pth)
	}

	// test '*'
	pth := "./test/*.txt"
	allSts, err := Glob(pth, nil)
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	}

	if len(allSts) != 2 {
		t.Errorf("expected %v got %v\n", 2, len(allSts))
	}

	// test '?'
	pth = "./test/file?.txt"
	allSts, err = Glob(pth, nil)
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	}

	if len(allSts) == 1 {
		f1 := allSts[0]
		expected := "file2.txt"
		_, got := path.Split(f1.Path)
		if got != expected {
			t.Errorf("expected file '%v' got '%v'\n", expected, got)
		}
	} else {
		t.Errorf("expected %v got %v\n", 1, len(allSts))
	}

	// test '?' with '*'
	pth = "./test/file?.*"
	allSts, err = Glob(pth, nil)
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	}

	if len(allSts) != 2 {
		t.Errorf("expected %v got %v\n", 2, len(allSts))
	}

	// cleanup
	for _, pth := range pths {
		rmFile(pth)
	}
	rmFile("./test")
}

func TestGlob_S3(t *testing.T) {
	// create files
	opt := NewOptions()
	opt.AWSAccessKey = testAccessKey
	opt.AWSSecretKey = testSecretKey
	pthDir := fmt.Sprintf("s3://%v/test", testBucket)
	pths := []string{
		fmt.Sprintf("%v/file-1.txt", pthDir),
		fmt.Sprintf("%v/file2.txt", pthDir),
		fmt.Sprintf("%v/file3.gz", pthDir),
	}
	for _, pth := range pths {
		createFile(pth)
	}

	// test '*'
	pth := fmt.Sprintf("%v/*.txt", pthDir)
	allSts, err := Glob(pth, opt)
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	}

	if len(allSts) != 2 {
		t.Errorf("expected %v got %v\n", 2, len(allSts))
	}

	// test '?'
	pth = fmt.Sprintf("%v/file?.txt", pthDir)
	allSts, err = Glob(pth, opt)
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	}

	if len(allSts) == 1 {
		f1 := allSts[0]
		expected := "file2.txt"
		_, got := path.Split(f1.Path)
		if got != expected {
			t.Errorf("expected file '%v' got '%v'\n", expected, got)
		}
	} else {
		t.Errorf("expected %v got %v\n", 1, len(allSts))
	}

	// test '?' with '*'
	pth = fmt.Sprintf("%v/file?.*", pthDir)
	allSts, err = Glob(pth, opt)
	if err != nil {
		t.Errorf("expected nil but got '%v'\n", err.Error())
	}

	if len(allSts) != 2 {
		t.Errorf("expected %v got %v\n", 2, len(allSts))
	}

	// cleanup
	for _, pth := range pths {
		rmS3File(pth)
	}
}

func TestMain(m *testing.M) {
	// setup
	s3.StoreHost = testEndpoint // set test endpoint

	// s3 client
	var err error
	testS3Client, err = minio.New(s3.StoreHost, testAccessKey, testSecretKey, true)
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

	code := m.Run()

	rmS3Bucket(testBucket)
	os.Exit(code)
}

func createFile(pth string) {
	opt := NewOptions()
	opt.AWSSecretKey = testSecretKey
	opt.AWSAccessKey = testAccessKey

	w, _ := NewWriter(pth, opt)
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	w.Close()
}

func rmFile(pth string) {
	fType := parseScheme(pth)

	if fType == "s3" {
		rmS3File(pth)
	}

	if fType == "nop" {
		return
	}

	os.Remove(pth)
}

func createBucket(bckt string) error {
	exists, err := testS3Client.BucketExists(bckt)
	if err != nil {
		return err
	}

	if exists {
		return nil
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
