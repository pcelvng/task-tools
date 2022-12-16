package nop

import (
	"fmt"
	"testing"
	"time"

	"github.com/hydronica/trial"

	"github.com/pcelvng/task-tools/file/stat"
)

func TestStat(t *testing.T) {
	tm := time.Now().UTC().Truncate(24 * time.Hour).Format(time.RFC3339)
	cases := trial.Cases[string, stat.Stats]{
		"file.txt": {
			Input: "nop://file.txt",
			Expected: stat.Stats{
				LineCnt:  10,
				Size:     123,
				Checksum: "28130874f9b9eb9711de4606399b7231",
				Created:  tm,
				Path:     "nop://file.txt"},
		},
		"error": {
			Input:     "nop://err",
			ShouldErr: true,
		},
		"directory": {
			Input: "nop://path/to/dir?stat_dir",
			Expected: stat.Stats{
				LineCnt:  10,
				Size:     123,
				Checksum: "0df5b11bdb0296d6f0646c840d64e738",
				Created:  tm,
				IsDir:    true,
				Path:     "nop://path/to/dir?stat_dir"},
		},
	}
	trial.New(Stat, cases).SubTest(t)
}

func ExampleNewReader() {
	// showing:
	// - new reader

	r, err := NewReader("nop://file.txt")
	if r == nil {
		return
	}
	fmt.Println(err)        // output: <nil>
	fmt.Println(r.sts.Path) // output: nop://file.txt

	// Output:
	// <nil>
	// nop://file.txt
}

func ExampleNewReaderErr() {
	// showing:
	// - new reader with err

	r, err := NewReader("nop://init_err")
	if err == nil {
		return
	}
	fmt.Println(r)           // output: <nil>
	fmt.Println(err.Error()) // output: init_err

	// Output:
	// <nil>
	// init_err
}

func ExampleReader_Read() {
	// showing:
	// - Reader.Read() happy path

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	fmt.Println(n)             // output: 10
	fmt.Println(err)           // output: <nil>
	fmt.Println(r.sts.ByteCnt) // output: 10

	// Output:
	// 10
	// <nil>
	// 10
}

func ExampleReader_ReadErr() {
	// showing:
	// - Reader.Read() with returned err

	r, _ := NewReader("nop://read_err")
	if r == nil {
		return
	}
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	fmt.Println(n)             // output: 0
	fmt.Println(err)           // output: read_err
	fmt.Println(r.sts.ByteCnt) // output: 0

	// Output:
	// 0
	// read_err
	// 0
}

func ExampleReader_ReadUsingMsgChan() {
	// showing:
	// - Reader.Read() where the message comes
	// from the message sent on MsgChan

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}
	mockLine := MockLine       // save to reset
	MockLine = make([]byte, 0) // set to len == 0 to use MsgChan
	go func() {
		MsgChan <- []byte("test msg") // len == 8
	}()

	buf := make([]byte, 100)
	n, err := r.Read(buf)
	fmt.Println(n)             // output: 8
	fmt.Println(err)           // output: <nil>
	fmt.Println(r.sts.ByteCnt) // output: 8

	MockLine = mockLine // reset MockLine

	// Output:
	// 8
	// <nil>
	// 8
}

func ExampleReader_ReadUsingEOFChan() {
	// showing:
	// - Reader.Read() with EOF returned error

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}

	// good practice to set and close
	// EOFChan to avoid panics.
	EOFChan = make(chan interface{})
	close(EOFChan)
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	fmt.Println(n)             // output: 10
	fmt.Println(err)           // output: EOF
	fmt.Println(r.sts.ByteCnt) // output: 10

	EOFChan = make(chan interface{}) // reset EOFChan

	// Output:
	// 10
	// EOF
	// 10
}

func ExampleReader_ReadLine() {
	// showing:
	// - Reader.ReadLine() happy path

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}
	ln, err := r.ReadLine()
	fmt.Print(string(ln))      // output: mock line
	fmt.Println(err)           // output: <nil>
	fmt.Println(r.sts.ByteCnt) // output: 10
	fmt.Println(r.sts.LineCnt) // output: 1

	// Output:
	// mock line
	// <nil>
	// 10
	// 1
}

func ExampleReader_ReadLineErr() {
	// showing:
	// - Reader.ReadLine() returning an error

	r, _ := NewReader("nop://readline_err")
	if r == nil {
		return
	}
	ln, err := r.ReadLine()
	fmt.Print(string(ln))      // output:
	fmt.Println(err)           // output: readline_err
	fmt.Println(r.sts.ByteCnt) // output: 0
	fmt.Println(r.sts.LineCnt) // output: 0

	// Output:
	//
	// readline_err
	// 0
	// 0
}

func ExampleReader_ReadLineUsingMsgChan() {
	// showing:
	// - Reader.Read() where the message comes
	// from the message sent on MsgChan

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}
	mockLine := MockLine       // save to reset
	MockLine = make([]byte, 0) // set to len == 0 to use MsgChan
	go func() {
		MsgChan <- []byte("test msg") // len == 8
	}()

	ln, err := r.ReadLine()
	fmt.Println(string(ln))    // output: test msg
	fmt.Println(err)           // output: <nil>
	fmt.Println(r.sts.ByteCnt) // output: 8
	fmt.Println(r.sts.LineCnt) // output: 1

	MockLine = mockLine // reset MockLine

	// Output:
	// test msg
	// <nil>
	// 8
	// 1
}

func ExampleReader_ReadLineUsingEOFChan() {
	// showing:
	// - Reader.Read() with EOF returned error

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}

	// good practice to set and close
	// EOFChan to avoid panics.
	EOFChan = make(chan interface{})
	close(EOFChan)

	ln, err := r.ReadLine()
	fmt.Print(string(ln))      // output: mock line
	fmt.Println(err)           // output: EOF
	fmt.Println(r.sts.ByteCnt) // output: 10
	fmt.Println(r.sts.LineCnt) // output: 1

	EOFChan = make(chan interface{}) // reset EOFChan

	// Output:
	// mock line
	// EOF
	// 10
	// 1
}

func ExampleReader_Stats() {
	// showing:
	// - Reader.Stats() happy path

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}
	r.sts.LineCnt = 10
	r.sts.ByteCnt = 100
	r.sts.Created = "created date"
	r.sts.Size = 200
	r.sts.Checksum = "checksum"

	sts := r.Stats()
	fmt.Println(sts.Path)     // output: nop://file.txt
	fmt.Println(sts.LineCnt)  // output: 10
	fmt.Println(sts.ByteCnt)  // output: 100
	fmt.Println(sts.Created)  // output: created date
	fmt.Println(sts.Size)     // output: 200
	fmt.Println(sts.Checksum) // output: checksum

	// Output:
	// nop://file.txt
	// 10
	// 100
	// created date
	// 200
	// checksum
}

func ExampleReader_Close() {
	// showing:
	// - Reader.ReadLine() returning an error

	r, _ := NewReader("nop://file.txt")
	if r == nil {
		return
	}
	r.sts.ByteCnt = 10
	err := r.Close()
	fmt.Println(err)        // output: <nil>
	fmt.Println(r.sts.Size) // output: 10

	// Output:
	// <nil>
	// 10
}

func ExampleReader_CloseErr() {
	// showing:
	// - Reader.ReadLine() returning an error

	r, _ := NewReader("nop://close_err")
	if r == nil {
		return
	}
	err := r.Close()
	fmt.Println(err) // output: close_err

	// Output:
	// close_err
}

func ExampleReader_AllErr() {
	// showing:
	// - all methods return err

	r, rErr := NewReader("nop://err")
	if r == nil {
		return
	}

	_, readErr := r.Read([]byte{})
	_, readlineErr := r.ReadLine()
	closeErr := r.Close()

	fmt.Println(rErr)        // output: <nil>
	fmt.Println(readErr)     // output: err
	fmt.Println(readlineErr) // output: err
	fmt.Println(closeErr)    // output: err

	// Output:
	// <nil>
	// err
	// err
	// err
}

func ExampleReader_MockReadMode() {
	// showing:
	// - reader uses MockReadMode set
	// directly on global variable.

	r := &Reader{
		MockReadMode: "err",
	}

	_, readErr := r.Read([]byte{})
	_, readlineErr := r.ReadLine()
	closeErr := r.Close()

	fmt.Println(readErr)     // output: err
	fmt.Println(readlineErr) // output: err
	fmt.Println(closeErr)    // output: err

	// Output:
	// err
	// err
	// err
}

func ExampleNewWriter() {
	// showing:
	// - new writer happy path

	w, err := NewWriter("nop://file.txt")
	if w == nil {
		return
	}
	fmt.Println(err)        // output: <nil>
	fmt.Println(w.sts.Path) // output: nop://file.txt

	// Output:
	// <nil>
	// nop://file.txt
}

func ExampleNewWriterErr() {
	// showing:
	// - new writer that returns a non-nil err

	w, err := NewWriter("nop://init_err")

	fmt.Println(w)   // output: <nil>
	fmt.Println(err) // output: init_err

	// Output:
	// <nil>
	// init_err
}

func ExampleWriter_Write() {
	// showing:
	// - writer.Write happy path

	w, _ := NewWriter("nop://file.txt")
	if w == nil {
		return
	}

	n, err := w.Write([]byte("test line"))
	fmt.Println(n)             // output: 9
	fmt.Println(err)           // output: <nil>
	fmt.Println(w.sts.ByteCnt) // output: 9
	fmt.Println(w.sts.LineCnt) // output: 0

	// Output:
	// 9
	// <nil>
	// 9
	// 0
}

func ExampleWriter_WriteErr() {
	// showing:
	// - writer.Write happy path

	w, _ := NewWriter("nop://write_err")
	if w == nil {
		return
	}

	n, err := w.Write([]byte("test line"))
	fmt.Println(n)             // output: 0
	fmt.Println(err)           // output: write_err
	fmt.Println(w.sts.ByteCnt) // output: 0
	fmt.Println(w.sts.LineCnt) // output: 0

	// Output:
	// 0
	// write_err
	// 0
	// 0
}

func ExampleWriter_WriteLine() {
	// showing:
	// - writer.WriteLine happy path

	w, _ := NewWriter("nop://file.txt")
	if w == nil {
		return
	}

	err := w.WriteLine([]byte("test line"))
	fmt.Println(err)           // output: <nil>
	fmt.Println(w.sts.ByteCnt) // output: 10
	fmt.Println(w.sts.LineCnt) // output: 1

	// Output:
	// <nil>
	// 10
	// 1
}

func ExampleWriter_WriteLineErr() {
	// showing:
	// - writer.WriteLine happy path

	w, _ := NewWriter("nop://writeline_err")
	if w == nil {
		return
	}

	err := w.WriteLine([]byte("test line"))
	fmt.Println(err)           // output: writeline_err
	fmt.Println(w.sts.ByteCnt) // output: 0
	fmt.Println(w.sts.LineCnt) // output: 0

	// Output:
	// writeline_err
	// 0
	// 0
}

func ExampleWriter_Stats() {
	// showing:
	// - writer.WriteLine happy path

	w, _ := NewWriter("nop://file.txt")
	if w == nil {
		return
	}

	sts := w.Stats()
	fmt.Println(sts.Path) // output: nop://file.txt

	// Output:
	// nop://file.txt
}

func ExampleWriter_Abort() {
	// showing:
	// - writer.Abort happy path

	w, _ := NewWriter("nop://file.txt")
	if w == nil {
		return
	}

	err := w.Abort()
	fmt.Println(err) // output: <nil>

	// Output:
	// <nil>
}

func ExampleWriter_AbortErr() {
	// showing:
	// - writer.Abort returning non-nil error

	w, _ := NewWriter("nop://abort_err")
	if w == nil {
		return
	}

	err := w.Abort()
	fmt.Println(err) // output: abort_err

	// Output:
	// abort_err
}

func ExampleWriter_Close() {
	// showing:
	// - writer.Close happy path

	w, _ := NewWriter("nop://file.txt")
	if w == nil {
		return
	}
	w.sts.ByteCnt = 10 // Size is set from final byte count
	err := w.Close()
	isCreated := w.sts.Created != "" // close sets sts.Created
	fmt.Println(err)                 // output: <nil>
	fmt.Println(w.sts.Size)          // output: 10
	fmt.Println(isCreated)           // output: true

	// Output:
	// <nil>
	// 10
	// true
}

func ExampleWriter_CloseErr() {
	// showing:
	// - writer.Close returns err

	w, _ := NewWriter("nop://close_err")
	if w == nil {
		return
	}
	w.sts.ByteCnt = 10 // Size is set from final byte count
	err := w.Close()
	isCreated := w.sts.Created != "" // close sets sts.Created
	fmt.Println(err)                 // output: <nil>
	fmt.Println(w.sts.Size)          // output: 0
	fmt.Println(isCreated)           // output: false

	// Output:
	// close_err
	// 0
	// false
}

func ExampleWriter_AllErr() {
	// showing:
	// - all methods return non-nil err

	w, wErr := NewWriter("nop://err")
	if w == nil {
		return
	}

	_, writeErr := w.Write([]byte("test"))
	writelineErr := w.WriteLine([]byte("test line"))
	abortErr := w.Abort()
	closeErr := w.Close()

	fmt.Println(wErr)         // output: <nil>
	fmt.Println(writeErr)     // output: err
	fmt.Println(writelineErr) // output: err
	fmt.Println(abortErr)     // output: err
	fmt.Println(closeErr)     // output: err

	// Output:
	// <nil>
	// err
	// err
	// err
	// err
}

func ExampleWriter_MockWriteMode() {
	// showing:
	// - reader uses MockWriteMode set
	// directly on global variable.

	w := &Writer{MockWriteMode: "err"}

	_, writeErr := w.Write([]byte("test"))
	writelineErr := w.WriteLine([]byte("test line"))
	abortErr := w.Abort()
	closeErr := w.Close()

	fmt.Println(writeErr)     // output: err
	fmt.Println(writelineErr) // output: err
	fmt.Println(abortErr)     // output: err
	fmt.Println(closeErr)     // output: err

	// Output:
	// err
	// err
	// err
	// err
}
