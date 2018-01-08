package s3

import (
	"errors"
	"fmt"
	"testing"
)

func ExamplePathErr_Error() {
	pthErr := PathErr("my/bad/pth")
	fmt.Println(pthErr.Error())

	// Output:
	// bad s3 path: my/bad/pth
}

func ExampleS3Err_Error() {
	err := errors.New("my s3 error")
	s3Err := S3Err{err}
	fmt.Println(s3Err.Error())

	// Output:
	// s3: my s3 error
}

func ExampleParsePth() {
	// showing:
	// - returned bucket
	// - returned object path

	pth := "s3://bucket/path/to/object.txt"
	bucket, objectPth, _ := parsePth(pth)
	fmt.Println(bucket)    // output: bucket
	fmt.Println(objectPth) // output: /path/to/object.txt

	// Output:
	// bucket
	// /path/to/object.txt
}

func TestParsePth(t *testing.T) {
	type inputOutput struct {
		inPth     string
		outBucket string
		outObjPth string
		pthErr    error
	}
	tests := []inputOutput{
		{"", "", "", PathErr("")},
		{"s3://", "", "", PathErr("s3://")},
		{"s3://bucket", "", "", PathErr("s3://bucket")},
		{"s3://bucket/", "bucket", "/", nil},
		{"s3://bucket/pth/to/object.txt", "bucket", "/pth/to/object.txt", nil},
	}

	for _, tst := range tests {
		bucket, objPth, err := parsePth(tst.inPth)
		if bucket != tst.outBucket || objPth != tst.outObjPth {
			t.Errorf(
				"for input '%v' expected bucket:objectPth '%v':'%v' but got '%v':'%v'",
				tst.inPth,
				tst.outBucket,
				tst.outObjPth,
				bucket,
				objPth,
			)
		}
		if tst.pthErr != err {
			t.Errorf(
				"expected err %v but got %v",
				tst.pthErr,
				err,
			)
		}
	}
}
