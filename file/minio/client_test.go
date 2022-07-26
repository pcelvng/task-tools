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
	// minio test server credentials
	//
	// see:
	// https://docs.minio.io/docs/golang-client-api-reference
	testBucket = "task-tools-test"
	testClient *minio.Client
	testOption = Option{
		Host:      "play.minio.io:9000",
		AccessKey: "Q3AM3UQ867SPQQA43P2F",
		SecretKey: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
		Secure:    true,
	}
)

func TestMain(m *testing.M) {
	var err error

	// test client
	testClient, err = newTestClient()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// make test bucket
	if err := createBucket(testBucket); err != nil {
		log.Fatal(err)
	}

	// create two test files for reading
	pth := fmt.Sprintf("mc://%v/read/test.txt", testBucket)
	if err := createTestFile(pth); err != nil {
		log.Fatal(err)
	}

	// compressed read test file
	gzPth := fmt.Sprintf("mc://%v/read/test.gz", testBucket)
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

	fn := func(i trial.Input) (interface{}, error) {
		_, b, v := parsePth(i.String())
		return []string{b, v}, nil
	}
	cases := trial.Cases{
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
