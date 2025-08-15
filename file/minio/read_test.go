package minio

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	minio "github.com/minio/minio-go/v7"

	"github.com/pcelvng/task-tools/file/stat"
)

func ExampleNewReader() {
	pth := fmt.Sprintf("mcs://%v/read/test.txt", testBucket)
	r, err := NewReader(pth, testOption)
	if r == nil {
		return
	}

	fmt.Println(err)          // <nil>
	fmt.Println(r.sts.Path()) // mcs://task-tools-test/read/test.txt
	fmt.Println(r.sts.Size)   // 20

	// Output:
	// <nil>
	// mcs://task-tools-test/read/test.txt
	// 20
}

func ExampleNewReaderErrBadObject() {
	r, err := NewReader("", testOption)

	fmt.Println(r)   // <nil>
	fmt.Println(err) // Bucket name cannot be empty

	// Output:
	// <nil>
	// Bucket name cannot be empty
}

func ExampleNewReaderErrObjStat() {
	pth := "mcs://does-not/exist.txt"
	r, err := NewReader(pth, testOption)

	fmt.Println(r)   // <nil>
	fmt.Println(err) // The specified bucket does not exist.

	// Output:
	// <nil>
	// The specified bucket does not exist
}

func ExampleNewReaderErrGzip() {
	// write a normal file to s3 as if it
	// were a gzip file. NewReader will see
	// the .gz extension and read it as a
	// gz file. Since it's not and there is
	// no gz header, it will return and error.

	// create 'bad' gz file.
	var buf bytes.Buffer
	buf.Write([]byte("test line\n"))
	buf.Write([]byte("test line\n"))
	opts := minio.PutObjectOptions{}
	opts.ContentType = "application/octet-stream"
	_, err := testClient.PutObject(
		context.Background(),
		testBucket,
		"bad.gz",
		&buf,
		20,
		opts,
	)
	if err != nil {
		log.Println(err)
		return
	}

	pth := fmt.Sprintf("mcs://%v/bad.gz", testBucket)
	r, err := NewReader(pth, testOption)

	fmt.Println(r)   // <nil>
	fmt.Println(err) // gzip: invalid header

	// cleanup file
	rmTestFile(pth)

	// Output:
	// <nil>
	// gzip: invalid header
}

func ExampleReader_Read() {
	pth := fmt.Sprintf("mcs://%v/read/test.txt", testBucket)
	r, err := NewReader(pth, testOption)
	if r == nil {
		return
	}

	b := make([]byte, 20)
	n, err := r.Read(b)

	fmt.Println(n)             // 20
	fmt.Println(err)           // <nil>
	fmt.Print(string(b))       // test line, test line
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 0

	// Output:
	// 20
	// <nil>
	// test line
	// test line
	// 20
	// 0
}

func ExampleReader_ReadCompressed() {
	pth := fmt.Sprintf("mcs://%v/read/test.gz", testBucket)
	r, _ := NewReader(pth, testOption)
	if r == nil {
		return
	}

	b := make([]byte, 20)
	n, err := r.Read(b)

	fmt.Println(n)             // 20
	fmt.Println(err)           // <nil>
	fmt.Print(string(b))       // test line, test line
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 0

	// Output:
	// 20
	// <nil>
	// test line
	// test line
	// 20
	// 0
}

func ExampleReader_ReadLine() {
	pth := fmt.Sprintf("mcs://%v/read/test.txt", testBucket)
	r, _ := NewReader(pth, testOption)
	if r == nil {
		return
	}

	ln1, err1 := r.ReadLine()
	ln2, err2 := r.ReadLine()

	fmt.Println(string(ln1))   // test line
	fmt.Println(err1)          // <nil>
	fmt.Println(string(ln2))   // test line
	fmt.Println(err2)          // <nil>
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 2

	// Output:
	// test line
	// <nil>
	// test line
	// <nil>
	// 20
	// 2
}

func ExampleReader_ReadLineCompressed() {
	pth := fmt.Sprintf("mcs://%v/read/test.gz", testBucket)
	r, _ := NewReader(pth, testOption)
	if r == nil {
		return
	}

	ln1, err1 := r.ReadLine()
	ln2, err2 := r.ReadLine()
	ln3, err3 := r.ReadLine() // EOF

	fmt.Println(string(ln1))   // test line
	fmt.Println(err1)          // <nil>
	fmt.Println(string(ln2))   // test line
	fmt.Println(err2)          // <nil>
	fmt.Println(string(ln3))   //
	fmt.Println(err3)          // EOF
	fmt.Println(r.sts.ByteCnt) // 20
	fmt.Println(r.sts.LineCnt) // 2

	// Output:
	// test line
	// <nil>
	// test line
	// <nil>
	//
	// EOF
	// 20
	// 2
}

func ExampleReader_Stats() {
	pth := fmt.Sprintf("mcs://%v/read/test.txt", testBucket)
	r, _ := NewReader(pth, testOption)
	if r == nil {
		return
	}

	r.ReadLine()
	sts := r.Stats()
	fmt.Println(sts.ByteCnt) // 10
	fmt.Println(sts.LineCnt) // 1

	// Output:
	// 10
	// 1
}

func ExampleReader_Close() {
	pth := fmt.Sprintf("mcs://%v/read/test.txt", testBucket)
	r, _ := NewReader(pth, testOption)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	r.ReadLine()
	err := r.Close()
	sts := r.Stats()

	fmt.Println(err)          // <nil>
	fmt.Println(sts.ByteCnt)  // 20
	fmt.Println(sts.LineCnt)  // 2
	fmt.Println(sts.Checksum) // 54f30d75cf7374c7e524a4530dbc93c2

	// Output:
	// <nil>
	// 20
	// 2
	// 54f30d75cf7374c7e524a4530dbc93c2
}

func ExampleReader_CloseCompressed() {
	pth := fmt.Sprintf("mcs://%v/read/test.gz", testBucket)
	r, _ := NewReader(pth, testOption)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	r.ReadLine()
	err := r.Close()
	sts := r.Stats()

	fmt.Println(err)          // <nil>
	fmt.Println(sts.ByteCnt)  // 20
	fmt.Println(sts.LineCnt)  // 2
	fmt.Println(sts.Checksum) // 42e649f9834028184ec21940d13a300f

	// Output:
	// <nil>
	// 20
	// 2
	// 42e649f9834028184ec21940d13a300f
}

func ExampleReader_CloseandClose() {
	pth := fmt.Sprintf("mcs://%v/read/test.gz", testBucket)
	r, _ := NewReader(pth, testOption)
	if r == nil {
		return
	}

	r.ReadLine()
	r.ReadLine()
	r.ReadLine()
	err1 := r.Close()
	err2 := r.Close()
	sts := r.Stats()

	fmt.Println(err1)         // <nil>
	fmt.Println(err2)         // <nil>
	fmt.Println(sts.ByteCnt)  // 20
	fmt.Println(sts.LineCnt)  // 2
	fmt.Println(sts.Checksum) // 42e649f9834028184ec21940d13a300f

	// Output:
	// <nil>
	// <nil>
	// 20
	// 2
	// 42e649f9834028184ec21940d13a300f
}

func TestListFiles(t *testing.T) {
	// setup - create objects
	pths := []string{
		fmt.Sprintf("mcs://%s/list-test/f1.txt", testBucket),
		fmt.Sprintf("mcs://%s/list-test/f2.txt", testBucket),
		fmt.Sprintf("mcs://%s/list-test/dir/f3.txt", testBucket),
	}

	for _, pth := range pths {
		createTestFile(pth)
	}

	// test returns only files
	dirPth := fmt.Sprintf("mcs://%s/list-test/", testBucket)
	allSts, err := ListFiles(dirPth, testOption)
	if err != nil {
		t.Error(err)
	}

	if len(allSts) != 3 {
		t.Fatalf("expected 3 files but got %v instead\n", len(allSts))
	}
	m := make(map[string]stat.Stats)
	for _, f := range allSts {
		m[strings.Replace(f.Path, dirPth, "", -1)] = f
		if !f.IsDir {
			if f.Created == "" {
				t.Errorf("%s should have created date", f.Path)
			}
			if f.Size == 0 {
				t.Errorf("%s should have size", f.Path)
			}
			if f.Checksum == "" {
				t.Errorf("%s should have checksum", f.Path)
			}
		}
	}
	f, ok := m["dir/"]
	if !ok {
		s := []string{}
		for key := range m {
			s = append(s, key)
		}
		t.Errorf("missing dir/ path, found %v", s)
	}
	if !f.IsDir {
		t.Errorf("should be dir")
	}

	// test that missing trailing "/" has same results
	dirPth = fmt.Sprintf("mcs://%s/list-test", testBucket)
	allSts, err = ListFiles(dirPth, testOption)
	if err != nil {
		t.Errorf("expected nil but got err '%v'\n", err.Error())
	}

	if len(allSts) != 3 {
		t.Fatalf("expected 3 files but got %v instead\n", len(allSts))
	}
	m = make(map[string]stat.Stats)
	for _, f := range allSts {
		m[strings.Replace(f.Path, dirPth, "", -1)] = f
		if !f.IsDir {
			if f.Created == "" {
				t.Errorf("%s should have created date", f.Path)
			}
			if f.Size == 0 {
				t.Errorf("%s should have size", f.Path)
			}
			if f.Checksum == "" {
				t.Errorf("%s should have checksum", f.Path)
			}
		}
	}
	f, ok = m["/dir/"]
	if !ok {
		s := []string{}
		for key := range m {
			s = append(s, key)
		}
		t.Errorf("missing /dir/ path, found %v", s)
	}
	if !f.IsDir {
		t.Errorf("should be dir")
	}

	// test bad s3 client
	_, err = ListFiles(dirPth, Option{Host: "bad/endpoint"})
	if err == nil {
		t.Error("expected err but got nil instead")
	}

	// cleanup
	for _, pth := range pths {
		rmTestFile(pth)
	}
}
