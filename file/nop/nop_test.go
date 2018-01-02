package nop

import (
	"fmt"
	"io"
)

func ExampleReader() {
	// showing:
	// - new reader
	// - stats path set
	// - Close sets Size from ByteCnt
	// - Close returns value of r.Err

	r := NewReader("file.txt")
	if r == nil {
		return
	}
	fmt.Println(r.sts.Path)    // file.txt
	fmt.Println(r.sts.ByteCnt) // 0
	fmt.Println(r.sts.Size)    // 0
	r.sts.ByteCnt = 10
	fmt.Println(r.Close())  // <nil>
	fmt.Println(r.sts.Size) // 10
	r.Err = io.EOF
	fmt.Println(r.Close()) // EOF

	// Output:
	// file.txt
	// 0
	// 0
	// <nil>
	// 10
	// EOF
}

func ExampleReader_ReadLine() {
	// showing:
	// - ReadLine returns a '1' for the line
	// - ReadLine increments line count by 1
	// - ReadLine increments byte count by 1
	// - ReadLine err = io.EOF when Err = io.EOF

	r := NewReader("")
	if r == nil {
		return
	}
	fmt.Println(r.sts.LineCnt) // 0
	fmt.Println(r.sts.ByteCnt) // 0
	ln, err := r.ReadLine()
	fmt.Println(string(ln)) // 1
	fmt.Println(err)        // <nil>
	sts := r.Stats()
	fmt.Println(sts.LineCnt) // 1
	fmt.Println(sts.ByteCnt) // 1
	r.Err = io.EOF
	_, err = r.ReadLine()
	fmt.Println(err) // EOF

	// Output:
	// 0
	// 0
	// 1
	// <nil>
	// 1
	// 1
	// EOF
}

func ExampleReader_Read() {
	// showing:
	// - Read '1' for first element of buffer
	// - Read returns n == 1
	// - Read increments byte count by 1
	// - Read returns n == 0 when len(buffer) == 0
	// - Read returns err == r.Err

	r := NewReader("")
	if r == nil {
		return
	}
	buf := make([]byte, 2)
	n, err := r.Read(buf)
	fmt.Println(n)              // 1
	fmt.Println(err)            // <nil>
	fmt.Println(string(buf[0])) // 1
	fmt.Println(buf[1])         // 0
	fmt.Println(r.sts.ByteCnt)  // 1
	n, err = r.Read([]byte{})
	fmt.Println(n) // 0
	r.Err = io.EOF
	_, err = r.Read([]byte{})
	fmt.Println(err) // EOF

	// Output:
	// 1
	// <nil>
	// 1
	// 0
	// 1
	// 0
	// EOF
}

func ExampleWriter() {
	// showing:
	// - new writer
	// - writer sts.Path set
	// - Stats returns stats
	// - Close returns w.Err
	// - Close sets sts.Size
	// - Abort returns w.Err

	w := NewWriter("file.txt")
	if w == nil {
		return
	}
	fmt.Println(w.sts.Path) // file.txt
	fmt.Println(w.sts.Size) // 0
	w.sts.ByteCnt = 10
	fmt.Println(w.Stats().Path)    // file.txt
	fmt.Println(w.Stats().ByteCnt) // 10
	fmt.Println(w.Close())         // <nil>
	fmt.Println(w.sts.Size)        // 10
	fmt.Println(w.Abort())         // <nil>
	w.Err = io.EOF
	fmt.Println(w.Close()) // EOF
	fmt.Println(w.Abort()) // EOF

	// Output:
	// file.txt
	// 0
	// file.txt
	// 10
	// <nil>
	// 10
	// <nil>
	// EOF
	// EOF
}

func ExampleWriter_WriteLine() {
	// showing:
	// - WriteLine increments LineCnt
	// - WriteLine increments ByteCnt
	// - WriteLine returns w.Err
	// - WriteLine does not increment LineCnt when w.Err != nil

	w := NewWriter("")
	if w == nil {
		return
	}

	fmt.Println(w.WriteLine([]byte("line"))) // <nil>
	fmt.Println(w.sts.LineCnt)               // 1
	fmt.Println(w.sts.ByteCnt)               // 5
	w.Err = io.EOF
	fmt.Println(w.WriteLine([]byte("line"))) // EOF
	fmt.Println(w.sts.LineCnt)               // 1
	fmt.Println(w.sts.ByteCnt)               // 10

	// Output:
	// <nil>
	// 1
	// 5
	// EOF
	// 1
	// 10
}

func ExampleWriter_Write() {
	// showing:
	// - Write returns n = len(p)
	// - Write increments ByteCnt by len(p)
	// - Write returns w.Err

	w := NewWriter("")
	if w == nil {
		return
	}

	n, err := w.Write([]byte("line"))
	fmt.Println(n)             // 4
	fmt.Println(w.sts.ByteCnt) // 4
	fmt.Println(err)           // <nil>
	w.Err = io.EOF
	n, err = w.Write([]byte("line"))
	fmt.Println(n)             // 4
	fmt.Println(w.sts.ByteCnt) // 8
	fmt.Println(err)           // EOF

	// Output:
	// 4
	// 4
	// <nil>
	// 4
	// 8
	// EOF
}
