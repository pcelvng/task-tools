package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/pcelvng/task-tools/file"
)

const (
	secret = ""
	access = ""
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

	ext := filepath.Ext(path)

	if ext == "mp4" || ext == "mkv" {

	}

	sts, err := file.Stat(path, &opt)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sts.JSONBytes())
}
