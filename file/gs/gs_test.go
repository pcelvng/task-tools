package gs

import (
	"fmt"
	"log"
	"os"
	"testing"

	minio "github.com/minio/minio-go"
)

var (
	// minio test server credentials
	//
	// see:
	// https://docs.minio.io/docs/golang-client-api-reference
	testEndpoint  = "play.minio.io:9000"
	testAccessKey = "Q3AM3UQ867SPQQA43P2F"
	testSecretKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	testBucket    = "task-tools-gstest"
	testGSClient  *minio.Client
)

func TestMain(m *testing.M) {
	var err error

	// switch to test endpoint
	StoreHost = testEndpoint

	// test client
	testGSClient, err = newTestGSClient()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// make test bucket
	if err := createBucket(testBucket); err != nil {
		log.Fatal(err)
	}

	// create two test files for reading
	pth := fmt.Sprintf("gs://%v/read/test.txt", testBucket)
	if err := createTestFile(pth); err != nil {
		log.Fatal(err)
	}

	// compressed read test file
	gzPth := fmt.Sprintf("gs://%v/read/test.gz", testBucket)
	if err := createTestFile(gzPth); err != nil {
		log.Fatal(err)
	}

	// run
	runRslt := m.Run()

	// remove read objects
	rmTestFile(pth)
	rmTestFile(gzPth)

	// remove test bucket
	rmBucket(testBucket)

	os.Exit(runRslt)
}

func newTestGSClient() (*minio.Client, error) {
	return newGSClient(testAccessKey, testSecretKey)
}

func createBucket(bckt string) error {
	exists, err := testGSClient.BucketExists(bckt)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	return testGSClient.MakeBucket(bckt, "us-east-1")
}

func rmBucket(bckt string) error {
	return testGSClient.RemoveBucket(bckt)
}

func createTestFile(pth string) error {
	w, err := newWriterFromGSClient(pth, testGSClient, nil)
	if err != nil {
		return err
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	err = w.Close()
	return err
}

func rmTestFile(pth string) error {
	bckt, objPth := parsePth(pth)
	return testGSClient.RemoveObject(bckt, objPth)
}

func ExampleParsePth() {
	// showing:
	// - returned bucket
	// - returned object path

	pth := "gs://bucket/path/to/object.txt"
	bucket, objectPth := parsePth(pth)
	fmt.Println(bucket)    // output: bucket
	fmt.Println(objectPth) // output: /path/to/object.txt

	// Output:
	// bucket
	// path/to/object.txt
}

func TestParsePth(t *testing.T) {
	type inputOutput struct {
		inPth     string
		outBucket string
		outObjPth string
	}
	tests := []inputOutput{
		{"", "", ""},
		{"gs://", "", ""},
		{"gs://bucket", "bucket", ""},
		{"gs://bucket/", "bucket", ""},
		{"gs://bucket/pth/to", "bucket", "pth/to"},
		{"gs://bucket/pth/to/", "bucket", "pth/to/"},
		{"gs://bucket/pth//to/", "bucket", "pth//to/"},
		{"gs://bucket/pth//to//", "bucket", "pth//to//"},
		{"gs://bucket/pth/to/object.txt", "bucket", "pth/to/object.txt"},
	}

	for _, tst := range tests {
		bucket, objPth := parsePth(tst.inPth)
		if bucket != tst.outBucket || objPth != tst.outObjPth {
			t.Errorf(
				"for input '%v' expected bucket:objectPth of %v:%v but got %v:%v",
				tst.inPth,
				tst.outBucket,
				tst.outObjPth,
				bucket,
				objPth,
			)
		}
	}
}

func TestStat(t *testing.T) {
	//setup
	dir := "gs://" + testBucket + "/stat/test/"
	file := "test.txt"
	path := dir + file
	t.Log(path)
	if err := createTestFile(path); err != nil {
		t.Fatal("setup", err)
	}

	t.Run("directory", func(t *testing.T) {
		s, err := Stat(dir, testAccessKey, testSecretKey)
		if err != nil {
			t.Error("directory", err)
		}
		if s.Path == "" {
			t.Error("directory stats: not set", s.JSONString())
		}
		if !s.IsDir {
			t.Error("dir: incorrect file type")
		}
	})

	t.Run("file", func(t *testing.T) {
		s, err := Stat(path, testAccessKey, testSecretKey)
		if err != nil {
			t.Error("file", err)
		}
		if s.Size == 0 || s.Path == "" || s.Created == "" {
			t.Error("file stats: not set", s.JSONString())
		}
		if s.IsDir {
			t.Error("file: incorrect file type")
		}
	})

	t.Run("missing", func(t *testing.T) {
		_, err := Stat(dir+"missing.txt", testAccessKey, testSecretKey)
		if err == nil {
			t.Error("Expected error on missing file")
		}
	})
}
