package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pcelvng/task-tools/file"
)

const (
	access = "hydronica"
	secret = "gernomiincan"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: Expected file")
	}
	path := os.Args[1]
	opt := file.Options{
		AccessKey: access,
		SecretKey: secret,
	}

	//sts, err := file.List(path, &opt)
	sts, err := file.Stat(path, &opt)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sts.JSONString())
}
