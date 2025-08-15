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

func TestNewReader(t *testing.T) {
	type output struct {
		Reader *Reader
		Path   string
	}

	fn := func(path string) (output, error) {
		r, err := NewReader(path)
		if err != nil {
			return output{}, err
		}
		return output{
			Reader: r,
			Path:   r.sts.Path(),
		}, nil
	}

	cases := trial.Cases[string, output]{
		"happy path": {
			Input: "nop://file.txt",
			Expected: output{
				Path: "nop://file.txt",
			},
		},
		"init error": {
			Input:     "nop://init_err",
			ShouldErr: true,
		},
	}

	trial.New(fn, cases).Comparer(
		trial.EqualOpt(trial.IgnoreFields("Reader")),
	).SubTest(t)
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
	fmt.Println(n)             // 10
	fmt.Println(err)           // <nil>
	fmt.Println(r.sts.ByteCnt) // 10

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
	fmt.Println(n)             // 0
	fmt.Println(err)           // read_err
	fmt.Println(r.sts.ByteCnt) // 0

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
	fmt.Println(n)             // 8
	fmt.Println(err)           // <nil>
	fmt.Println(r.sts.ByteCnt) // 8

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
	fmt.Println(n)             // 10
	fmt.Println(err)           // EOF
	fmt.Println(r.sts.ByteCnt) // 10

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
	fmt.Print(string(ln))      // mock line
	fmt.Println(err)           // <nil>
	fmt.Println(r.sts.ByteCnt) // 10
	fmt.Println(r.sts.LineCnt) // 1

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
	fmt.Print(string(ln))      //
	fmt.Println(err)           // readline_err
	fmt.Println(r.sts.ByteCnt) // 0
	fmt.Println(r.sts.LineCnt) // 0

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
	fmt.Println(string(ln))    // test msg
	fmt.Println(err)           // <nil>
	fmt.Println(r.sts.ByteCnt) // 8
	fmt.Println(r.sts.LineCnt) // 1

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
	fmt.Print(string(ln))      // mock line
	fmt.Println(err)           // EOF
	fmt.Println(r.sts.ByteCnt) // 10
	fmt.Println(r.sts.LineCnt) // 1

	EOFChan = make(chan interface{}) // reset EOFChan

	// Output:
	// mock line
	// EOF
	// 10
	// 1
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
	fmt.Println(err)        // <nil>
	fmt.Println(r.sts.Size) // 10

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
	fmt.Println(err) // close_err

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

	fmt.Println(rErr)        // <nil>
	fmt.Println(readErr)     // err
	fmt.Println(readlineErr) // err
	fmt.Println(closeErr)    // err

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

	r, _ := NewReader("err://err")

	_, readErr := r.Read([]byte{})
	_, readlineErr := r.ReadLine()
	closeErr := r.Close()

	fmt.Println(readErr)     // err
	fmt.Println(readlineErr) // err
	fmt.Println(closeErr)    // err

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
	fmt.Println(err)          // <nil>
	fmt.Println(w.sts.Path()) // nop://file.txt

	// Output:
	// <nil>
	// nop://file.txt
}

func ExampleNewWriterErr() {
	// showing:
	// - new writer that returns a non-nil err

	w, err := NewWriter("nop://init_err")

	fmt.Println(w)   // <nil>
	fmt.Println(err) // init_err

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
	fmt.Println(n)             // 9
	fmt.Println(err)           // <nil>
	fmt.Println(w.sts.ByteCnt) // 9
	fmt.Println(w.sts.LineCnt) // 0

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
	fmt.Println(n)             // 0
	fmt.Println(err)           // write_err
	fmt.Println(w.sts.ByteCnt) // 0
	fmt.Println(w.sts.LineCnt) // 0

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
	fmt.Println(err)           // <nil>
	fmt.Println(w.sts.ByteCnt) // 10
	fmt.Println(w.sts.LineCnt) // 1

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
	fmt.Println(err)           // writeline_err
	fmt.Println(w.sts.ByteCnt) // 0
	fmt.Println(w.sts.LineCnt) // 0

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
	fmt.Println(sts.Path) // nop://file.txt

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
	fmt.Println(err) // <nil>

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
	fmt.Println(err) // abort_err

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
	isCreated := w.sts.Created() != "" // close sets sts.Created
	fmt.Println(err)                   // <nil>
	fmt.Println(w.sts.Size)            // 10
	fmt.Println(isCreated)             // true

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
	isCreated := w.sts.Created() != "" // close sets sts.Created
	fmt.Println(err)                   // <nil>
	fmt.Println(w.sts.Size)            // 0
	fmt.Println(isCreated)             // false

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

	fmt.Println(wErr)         // <nil>
	fmt.Println(writeErr)     // err
	fmt.Println(writelineErr) // err
	fmt.Println(abortErr)     // err
	fmt.Println(closeErr)     // err

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

	fmt.Println(writeErr)     // err
	fmt.Println(writelineErr) // err
	fmt.Println(abortErr)     // err
	fmt.Println(closeErr)     // err

	// Output:
	// err
	// err
	// err
	// err
}
