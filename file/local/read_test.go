package local

import "fmt"

func ExampleReader_Readline() {
	r := &Reader{}
	r.ReadLine()
	fmt.Println("done")

	// Output: done
}
