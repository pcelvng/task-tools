package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/pcelvng/task-tools/file"
)

var path = flag.String("p", "", "list path")

func main() {
	flag.Parse()
	opts := &file.Options{
		AccessKey: "GOOGUENBQDOVNQHGTEF33LHD",
		SecretKey: "dhY5/zeHveEPsYv9VsRVnmQd3gB4lpHhod+/7ypy",
	}
	sts, err := file.List(*path, opts)
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range sts {
		fmt.Printf("%-40s\t%v\t%v\t%v\n", s.Path, s.IsDir, s.Size, s.Checksum)
	}
}
