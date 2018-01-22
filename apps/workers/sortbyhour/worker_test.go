package main

import "fmt"

func ExampleNewInfoOptions() {
	info := `nop://source/file.txt?record-type=json&date-field=fieldName&dest-template=nop://dest/{HH}.json`
	iOpt, err := newInfoOptions(info)

	fmt.Println(err)          // output: <nil>
	fmt.Println(iOpt.SrcPath) // output: nop://source/file.txt

	// Output:
	// <nil>
	// nop://source/file.txt
}
