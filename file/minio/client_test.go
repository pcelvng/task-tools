package minio

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/hydronica/trial"
	minio "github.com/minio/minio-go/v7"
)

var (
	testBucket = "task-tools-test"
	testClient *minio.Client
)

var testOption = Option{
	Host:      getEnv("MINIO_ENDPOINT", "localhost:9000"),
	AccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
	SecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
	Secure:    false, // local Docker instance uses HTTP
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func TestMain(m *testing.M) {
	var err error

	log.SetFlags(log.Llongfile)
	// test client
	testClient, err = newTestClient()
	if err != nil {
		fmt.Println("\033[34mSKIP: Minio server not available - run 'docker run -p 9000:9000 minio/minio server /data' for local testing\033[0m")
		return
	}

	// make test bucket
	if err := createBucket(testBucket); err != nil {
		fmt.Printf("\033[34mSKIP: error creating bucket: %v\033[0m\n", err)
		return
	}

	// create test files for reading
	readFiles := []string{
		fmt.Sprintf("mc://%v/read/test.txt", testBucket),
		fmt.Sprintf("mc://%v/read/test.gz", testBucket),
	}
	for _, pth := range readFiles {
		if err := createTestFile(pth); err != nil {
			fmt.Printf("\033[34mSKIP: error creating %s: %v\033[0m\n", pth, err)
			return
		}
	}

	// create test files for glob testing
	globFiles := []string{
		fmt.Sprintf("mc://%v/glob/file-1.txt", testBucket),
		fmt.Sprintf("mc://%v/glob/file2.txt", testBucket),
		fmt.Sprintf("mc://%v/glob/file3.gz", testBucket),
		fmt.Sprintf("mc://%v/glob/f1/file4.gz", testBucket),
		fmt.Sprintf("mc://%v/glob/f3/file5.txt", testBucket),
		fmt.Sprintf("mc://%v/glob/f5/file-6.txt", testBucket),
	}
	for _, pth := range globFiles {
		if err := createTestFile(pth); err != nil {
			fmt.Printf("\033[34mSKIP: error creating %s: %v\033[0m\n", pth, err)
			return
		}
	}

	// run
	runRslt := m.Run()

	// remove test objects
	for _, pth := range readFiles {
		rmTestFile(pth)
	}
	for _, pth := range globFiles {
		rmTestFile(pth)
	}

	// remove test bucket
	rmBucket(testBucket)

	os.Exit(runRslt)
}

func newTestClient() (*minio.Client, error) {
	return newClient(testOption)
}

func createBucket(bckt string) error {
	exists, err := testClient.BucketExists(context.Background(), bckt)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	return testClient.MakeBucket(context.Background(), bckt, minio.MakeBucketOptions{})
}

func rmBucket(bckt string) error {
	return testClient.RemoveBucket(context.Background(), bckt)
}

func createTestFile(pth string) error {
	w, err := newWriterFromClient(pth, testClient, nil)
	if err != nil {
		return err
	}
	w.WriteLine([]byte("test line"))
	w.WriteLine([]byte("test line"))
	err = w.Close()
	return err
}

func rmTestFile(pth string) error {
	_, bckt, objPth := parsePth(pth)
	return testClient.RemoveObject(context.Background(), bckt, objPth, minio.RemoveObjectOptions{})
}

func TestParsePth(t *testing.T) {
	type inputOutput struct {
		inPth     string
		outBucket string
		outObjPth string
	}

	fn := func(i string) ([]string, error) {
		_, b, v := parsePth(i)
		return []string{b, v}, nil
	}
	cases := trial.Cases[string, []string]{
		"empty": {
			Input:    "",
			Expected: []string{"", ""},
		},
		"bucket only": {
			Input:    "mc://bucket",
			Expected: []string{"bucket", ""},
		},
		"bucket/": {
			Input:    "mc://bucket/",
			Expected: []string{"bucket", ""},
		},
		"full path": {
			Input:    "mc://bucket/pth/to/object.txt",
			Expected: []string{"bucket", "pth/to/object.txt"},
		},
		"host:port+bucket": {
			Input:    "mc://127.0.0.1:80/bucket",
			Expected: []string{"bucket", ""},
		},
		"host:port+bucket/": {
			Input:    "mc://127.0.0.1:81/bucket/",
			Expected: []string{"bucket", ""},
		},
		"host:port+bucket+path": {
			Input:    "mc://127.0.0.1:81/bucket/path/to/file.txt",
			Expected: []string{"bucket", "path/to/file.txt"},
		},
	}
	trial.New(fn, cases).SubTest(t)

}

func TestStat(t *testing.T) {
	//setup
	dir := "mc://" + testBucket + "/stat/test/"
	file := "test.txt"
	path := dir + file
	t.Log(path)
	if err := createTestFile(path); err != nil {
		t.Fatal("setup", err)
	}

	t.Run("directory", func(t *testing.T) {
		s, err := Stat(dir, testOption)
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
		s, err := Stat(path, testOption)
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
		_, err := Stat(dir+"missing.txt", testOption)
		if err == nil {
			t.Error("Expected error on missing file")
		}
	})
}
