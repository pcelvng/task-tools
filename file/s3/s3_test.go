package s3

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/minio/minio-go"
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

func TestMain(m *testing.M) {
	var err error

	// switch to test endpoint
	storeEndpoint = testEndpoint

	// test client
	testS3Client, err = minio.New(testEndpoint, testAccessKey, testSecretKey, true)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// make test bucket
	err = createBucket(testBucket)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// create two test files for reading
	pth := fmt.Sprintf("s3://%v/read/test.txt", testBucket)
	err = createTestFile(pth)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// compressed read test file
	gzPth := fmt.Sprintf("s3://%v/read/test.gz", testBucket)
	err = createTestFile(gzPth)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// run
	runRslt := m.Run()

	// remove read objects
	rmTestFile(pth)
	rmTestFile(gzPth)

	// remove test bucket
	err = rmBucket(testBucket)
	if err != nil {
		log.Println(err.Error())
	}

	os.Exit(runRslt)
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

func rmBucket(bckt string) error {
	return testS3Client.RemoveBucket(bckt)
}

func ExampleParsePth() {
	// showing:
	// - returned bucket
	// - returned object path

	pth := "s3://bucket/path/to/object.txt"
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
		{"s3://", "", ""},
		{"s3://bucket", "bucket", ""},
		{"s3://bucket/", "bucket", ""},
		{"s3://bucket/pth/to/object.txt", "bucket", "pth/to/object.txt"},
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
